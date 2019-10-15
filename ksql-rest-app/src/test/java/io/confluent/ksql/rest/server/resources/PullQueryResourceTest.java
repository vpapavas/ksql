/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorCode;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.ws.rs.core.Response;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryResourceTest {

  private static final Duration DISCONNECT_CHECK_INTERVAL = Duration.ofMillis(1000);
  private static final Duration COMMAND_QUEUE_CATCHUP_TIMOEUT = Duration.ofMillis(1000);
  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f1"), SqlTypes.INTEGER)
      .build();

  private static final KsqlConfig VALID_CONFIG = new KsqlConfig(ImmutableMap.of(
      StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1"
  ));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlEngine mockKsqlEngine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient mockKafkaTopicClient;
  @Mock
  private StatementParser mockStatementParser;
  @Mock
  private CommandQueue commandQueue;
  @Mock
  private ActivenessRegistrar activenessRegistrar;
  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;
  @Mock
  private KsqlAuthorizationValidator authorizationValidator;
  private PullQueryResource testResource;

  private final static String queryString = "SELECT * FROM test_stream EMIT CHANGES limit 5;";
  private final static String printString = "Print TEST_TOPIC;";
  private final static String topicName = "test_stream";
  private PreparedStatement<Statement> statement;

  @Before
  public void setup() {
    when(serviceContext.getTopicClient()).thenReturn(mockKafkaTopicClient);
    statement = PreparedStatement.of("s", mock(Statement.class));
    when(mockStatementParser.parseSingleStatement(queryString)).thenReturn(statement);

    testResource = new PullQueryResource(
        mockKsqlEngine,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        authorizationValidator
    );

    testResource.configure(VALID_CONFIG);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnConfigureIfAppServerNotSet() {
    // Given:
    final KsqlConfig configNoAppServer = new KsqlConfig(ImmutableMap.of());

    // When:
    testResource.configure(configNoAppServer);
  }

  @Test
  public void shouldThrowOnHandleStatementIfNotConfigured() {
    // Given:
    testResource = new PullQueryResource(
        mockKsqlEngine,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        authorizationValidator
    );

    // Then:
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.SERVICE_UNAVAILABLE)));
    expectedException
        .expect(exceptionErrorMessage(errorMessage(Matchers.is("Server initializing"))));

    // When:
    testResource.pullQuery(
        serviceContext,
        new KsqlRequest("pull", Collections.emptyMap(), null)
    );
  }

  @Test
  public void shouldReturn400OnBadStatement() {
    // Given:
    when(mockStatementParser.parseSingleStatement(any()))
        .thenThrow(new IllegalArgumentException("some error message"));

    // Expect
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.BAD_REQUEST)));
    expectedException.expect(exceptionErrorMessage(errorMessage(is("some error message"))));
    expectedException.expect(
        exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_BAD_STATEMENT))));

    // When:
    testResource.pullQuery(
        serviceContext,
        new KsqlRequest("pull", Collections.emptyMap(), null)
    );
  }

  @Test
  public void shouldNotWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.pullQuery(
        serviceContext,
        new KsqlRequest(queryString, Collections.emptyMap(), null)
    );

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.pullQuery(
        serviceContext,
        new KsqlRequest(queryString, Collections.emptyMap(), 3L)
    );

    // Then:
    verify(commandQueue).ensureConsumedPast(eq(3L), any());
  }

  @Test
  public void shouldReturnServiceUnavailableIfTimeoutWaitingForCommandSequenceNumber()
      throws Exception {
    // Given:
    doThrow(new TimeoutException("whoops"))
        .when(commandQueue).ensureConsumedPast(anyLong(), any());

    // Expect
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.SERVICE_UNAVAILABLE)));
    expectedException.expect(exceptionErrorMessage(errorMessage(
        containsString("Timed out while waiting for a previous command to execute"))));
    expectedException.expect(
        exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT))));

    // When:
    testResource.pullQuery(
        serviceContext,
        new KsqlRequest(queryString, Collections.emptyMap(), 3L)
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldStreamRowsCorrectly() throws Throwable {
    final int NUM_ROWS = 5;
    final AtomicReference<Throwable> threadException = new AtomicReference<>(null);
    final Thread.UncaughtExceptionHandler threadExceptionHandler =
        (thread, exception) -> threadException.compareAndSet(null, exception);

    final String queryString = "SELECT * FROM test_stream limit 5;";

    final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue = new SynchronousQueue<>();

    final LinkedList<GenericRow> writtenRows = new LinkedList<>();

    final Thread rowQueuePopulatorThread = new Thread(() -> {
      try {
        for (int i = 0; i != NUM_ROWS; i++) {
          final String key = Integer.toString(i);
          final GenericRow value = new GenericRow(Collections.singletonList(i));
          synchronized (writtenRows) {
            writtenRows.add(value);
          }
          rowQueue.put(new KeyValue<>(key, value));
        }
      } catch (final InterruptedException exception) {
        // This should happen during the test, so it's fine
      }
    }, "Row Queue Populator");
    rowQueuePopulatorThread.setUncaughtExceptionHandler(threadExceptionHandler);
    rowQueuePopulatorThread.start();

    final KafkaStreams mockKafkaStreams = mock(KafkaStreams.class);

    final Map<String, Object> requestStreamsProperties = Collections.emptyMap();

    statement = PreparedStatement.of("query", mock(Query.class));
    when(mockStatementParser.parseSingleStatement(queryString))
        .thenReturn(statement);

    final TransientQueryMetadata transientQueryMetadata =
        new TransientQueryMetadata(
            queryString,
            mockKafkaStreams,
            SOME_SCHEMA,
            Collections.emptySet(),
            limitHandler -> {},
            "",
            rowQueue,
            "",
            mock(Topology.class),
            Collections.emptyMap(),
            Collections.emptyMap(),
            queryCloseCallback);

    when(mockKsqlEngine.execute(serviceContext,
        ConfiguredStatement.of(statement, requestStreamsProperties, VALID_CONFIG)))
        .thenReturn(ExecuteResult.of(transientQueryMetadata));

    final Response response =
        testResource.pullQuery(
            serviceContext,
            new KsqlRequest(queryString, requestStreamsProperties, null)
        );

    assertThat(response.getEntity(), instanceOf(String.class));
    System.out.println(response.getEntity());

    rowQueuePopulatorThread.interrupt();
    rowQueuePopulatorThread.join();

    // Definitely want to make sure that the Kafka Streams instance has been closed and cleaned up
    verify(mockKafkaStreams).start();
    verify(mockKafkaStreams).setUncaughtExceptionHandler(any());
    verify(mockKafkaStreams).cleanUp();
    verify(mockKafkaStreams).close();

    // If one of the other threads has somehow managed to throw an exception without breaking things up until this
    // point, we throw that exception now in the main thread and cause the test to fail
    final Throwable exception = threadException.get();
    if (exception != null) {
      throw exception;
    }
  }

}
