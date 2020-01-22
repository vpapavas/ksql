/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.ActiveStandbyResponse;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ActiveStandbyResourceTest {

  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private PersistentQueryMetadata query;
  @Mock
  private StreamsMetadata streamsMetadata;

  private static final String APPLICATION_SERVER_ID = "http://localhost:8088";
  private HostInfo hostInfo;
  private List<StreamsMetadata> allMetadata;
  private ActiveStandbyResource activeStandbyResource;

  @Before
  public void setUp() {
    hostInfo = ServerUtil.parseHostInfo(APPLICATION_SERVER_ID);
    activeStandbyResource = new ActiveStandbyResource(ksqlEngine);
    activeStandbyResource.setLocalHostInfo(APPLICATION_SERVER_ID);
    allMetadata = ImmutableList.of(streamsMetadata);
  }

  @Test
  public void shouldReturnResponse() {
    // When:
    final Response response = activeStandbyResource.getActiveStandbyInformation();

    // Then:
    assertThat(response.getStatus(), is(200));
    assertThat(response.getEntity(), instanceOf(ActiveStandbyResponse.class));
  }

  @Test
  public void shouldReturnActiveAndStandByStores() {
    // Given:
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(query));
    when(query.getAllMetadata()).thenReturn(allMetadata);
    when(((QueryMetadata)query).getQueryApplicationId()).thenReturn("test");
    when(streamsMetadata.hostInfo()).thenReturn(hostInfo);
    when(streamsMetadata.stateStoreNames()).thenReturn(ImmutableSet.of("activeStore1", "activeStore2"));
    when(streamsMetadata.standbyStateStoreNames()).thenReturn(ImmutableSet.of("standByStore1", "standByStore2"));

    // When:
    final ActiveStandbyResponse response = activeStandbyResource.getResponse();

    // Then:
    assertThat(response.getPerQueryInfo().get("test").getActiveStores().containsAll(
        ImmutableSet.of("activeStore1", "activeStore2")), is(true));
    assertThat(response.getPerQueryInfo().get("test").getStandByStores().containsAll(
        ImmutableSet.of("standByStore1", "standByStore2")), is(true));
  }

}
