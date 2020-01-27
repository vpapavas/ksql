/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.ActiveStandbyEntity;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.UserDataProvider;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

/**
 * Test to ensure pull queries route across multiple KSQL nodes correctly.
 *
 * <p>For tests on general syntax and handled see RestQueryTranslationTest's
 * materialized-aggregate-static-queries.json
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
@Category({IntegrationTest.class})
public class PullQueryRoutingFunctionalTest {

  private static final TemporaryFolder TMP = new TemporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }
  private static final HostInfoEntity host0 = new HostInfoEntity("localhost", 8088);
  private static final HostInfoEntity host1 = new HostInfoEntity("localhost",8089);
  private static final HostInfoEntity host2 = new HostInfoEntity("localhost",8087);
  private static final String USER_TOPIC = "user_topic";
  private static final String USERS_STREAM = "users";
  private static final UserDataProvider USER_PROVIDER = new UserDataProvider();
  private static final Format VALUE_FORMAT = Format.JSON;
  private static final int HEADER = 1;
  private static String output;
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final int BASE_TIME = 1_000_000;
  private static final String QUERY_ID = "_confluent-ksql-default_query_CTAS_ID_0_0";

  private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
          .build(),
      SerdeOption.none()
  );

  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      .withProperty(KsqlConfig.KSQL_QUERY_PULL_ALLOW_STALE_READS, true)
      .withProperty(KsqlConfig.KSQL_STREAMS_PREFIX + "num.standby.replicas", 1)
      .build();

  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      .withProperty(KsqlConfig.KSQL_QUERY_PULL_ALLOW_STALE_READS, true)
      .withProperty(KsqlConfig.KSQL_STREAMS_PREFIX + "num.standby.replicas", 1)
      .build();

  private static final TestKsqlRestApp REST_APP_2 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8087")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8087")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      .withProperty(KsqlConfig.KSQL_QUERY_PULL_ALLOW_STALE_READS, true)
      .withProperty(KsqlConfig.KSQL_STREAMS_PREFIX + "num.standby.replicas", 1)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP_0)
      .around(REST_APP_1)
      .around(REST_APP_2);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    //Create topic with 1 partition to control who is active and standby
    TEST_HARNESS.ensureTopics(1, USER_TOPIC);

    final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);

    TEST_HARNESS.produceRows(
        USER_TOPIC,
        USER_PROVIDER,
        VALUE_FORMAT,
        timestampSupplier::getAndIncrement
    );

    //Create stream
    makeAdminRequest(
        "CREATE STREAM " + USERS_STREAM
            + " (" + USER_PROVIDER.ksqlSchemaString() + ")"
            + " WITH ("
            + "   kafka_topic='" + USER_TOPIC + "', "
            + "   key='" + USER_PROVIDER.key() + "', "
            + "   value_format='" + VALUE_FORMAT + "'"
            + ");"
    );
  }

  @Before
  public void setUp() {
    REST_APP_0.start();
    REST_APP_1.start();
    REST_APP_2.start();
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();

  }

  @After
  public void cleanUp() {
    REST_APP_0.stop();
    REST_APP_1.stop();
    REST_APP_2.stop();
  }

  @Test(timeout = 60000)
  public void shouldQueryActiveWhenActiveAliveQueryIssuedToStandby() {
    // Given:
    final String key = Iterables.get(USER_PROVIDER.data().keySet(), 0);
    final String sql = "SELECT * FROM " + output + " WHERE ROWKEY = '" + key + "';";
    makeAdminRequest(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );
    waitForTableRows();
    HighAvailabilityTestUtil.waitForClusterToBeDiscovered(REST_APP_0, 3);
    HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize(
        REST_APP_0, ImmutableList.of(host0, host1, host2), QUERY_ID);
    ClusterFormation clusterFormation = findClusterFormation();
    HighAvailabilityTestUtil.sendHeartbeartsForWindowLength(
        clusterFormation.standBy.right, clusterFormation.active.left, 2000);
    HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        clusterFormation.standBy.right, clusterFormation.active.left, HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.standBy.right, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().getColumns(), is(ImmutableList.of(key, BASE_TIME, 1)));
  }

  @Test(timeout = 60000)
  public void shouldQueryActiveWhenActiveAliveStandbyDeadQueryIssuedToRouter() {
    // Given:
    final String key = Iterables.get(USER_PROVIDER.data().keySet(), 0);
    final String sql = "SELECT * FROM " + output + " WHERE ROWKEY = '" + key + "';";
    makeAdminRequest(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );
    waitForTableRows();
    HighAvailabilityTestUtil.waitForClusterToBeDiscovered(REST_APP_0, 3);
    HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize(
        REST_APP_0, ImmutableList.of(host0, host1, host2), QUERY_ID);
    ClusterFormation clusterFormation = findClusterFormation();
    HighAvailabilityTestUtil.sendHeartbeartsForWindowLength(
        clusterFormation.router.right, clusterFormation.active.left, 2000);
    HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        clusterFormation.router.right,
        clusterFormation.active.left,
        HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.right, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().getColumns(), is(ImmutableList.of(key, BASE_TIME, 1)));
  }

  @Test(timeout = 60000)
  public void shouldQueryStandbyWhenActiveDeadStandbyAliveQueryIssuedToRouter() {
    // Given:
    final String key = Iterables.get(USER_PROVIDER.data().keySet(), 0);
    final String sql = "SELECT * FROM " + output + " WHERE ROWKEY = '" + key + "';";
    makeAdminRequest(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );
    waitForTableRows();
    HighAvailabilityTestUtil.waitForClusterToBeDiscovered(REST_APP_0, 3);
    HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize(
        REST_APP_0, ImmutableList.of(host0, host1, host2), QUERY_ID);
    ClusterFormation clusterFormation = findClusterFormation();
    HighAvailabilityTestUtil.sendHeartbeartsForWindowLength(
        clusterFormation.router.right, clusterFormation.standBy.left, 2000);
    HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        clusterFormation.router.right,
        clusterFormation.standBy.left,
        HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.right, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().getColumns(), is(ImmutableList.of(key, BASE_TIME, 1)));
  }

  private static List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, Optional.empty());
  }

  private static void makeAdminRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP_0, sql, Optional.empty());
  }

  private static ClusterFormation findClusterFormation() {
    ClusterFormation clusterFormation = new ClusterFormation();
    ClusterStatusResponse clusterStatusResponse = HighAvailabilityTestUtil.sendClusterStatusRequest(REST_APP_0);
    ActiveStandbyEntity entity0 = clusterStatusResponse.getClusterStatus().get(host0)
        .getPerQueryActiveStandbyEntity().get(QUERY_ID);
    ActiveStandbyEntity entity1 = clusterStatusResponse.getClusterStatus().get(host1)
        .getPerQueryActiveStandbyEntity().get(QUERY_ID);
    ActiveStandbyEntity entity2 = clusterStatusResponse.getClusterStatus().get(host2)
        .getPerQueryActiveStandbyEntity().get(QUERY_ID);

    // find active
    if(!entity0.getActiveStores().isEmpty() && !entity0.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(Pair.of(host0, REST_APP_0));
    }
    else if(!entity1.getActiveStores().isEmpty() && !entity1.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(Pair.of(host1, REST_APP_1));
    } else {
      clusterFormation.setActive(Pair.of(host2, REST_APP_2));
    }

    //find standby
    if(!entity0.getStandByStores().isEmpty() && !entity0.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(Pair.of(host0, REST_APP_0));
    }
    else if(!entity1.getStandByStores().isEmpty() && !entity1.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(Pair.of(host1, REST_APP_1));
    } else {
      clusterFormation.setStandBy(Pair.of(host2, REST_APP_2));
    }

    //find router
    if(entity0.getStandByStores().isEmpty() && entity0.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(Pair.of(host0, REST_APP_0));
    }
    else if(entity1.getStandByStores().isEmpty() && entity1.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(Pair.of(host1, REST_APP_1));
    } else {
      clusterFormation.setRouter(Pair.of(host2, REST_APP_2));
    }

    return clusterFormation;
  }

  static class ClusterFormation {
    Pair<HostInfoEntity, TestKsqlRestApp> active;
    Pair<HostInfoEntity, TestKsqlRestApp> standBy;
    Pair<HostInfoEntity, TestKsqlRestApp> router;

    ClusterFormation() {
    }

    public void setActive(final Pair<HostInfoEntity, TestKsqlRestApp> active) {
      this.active = active;
    }

    public void setStandBy(final Pair<HostInfoEntity, TestKsqlRestApp> standBy) {
      this.standBy = standBy;
    }

    public void setRouter(final Pair<HostInfoEntity, TestKsqlRestApp> router) {
      this.router = router;
    }

    public String toString() {
      return new StringBuilder()
          .append("Active = ").append(active.left)
          .append(", Standby = ").append(standBy.left)
          .append(", Router = ").append(router.left)
          .toString();
    }
  }

  private void waitForTableRows() {
    TEST_HARNESS.verifyAvailableUniqueRows(
        output.toUpperCase(),
        USER_PROVIDER.data().size(),
        VALUE_FORMAT,
        AGGREGATE_SCHEMA
    );
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}

