/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.ActiveStandbyEntity;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.HeartbeatAgent;
import io.confluent.ksql.rest.server.HeartbeatAgent.HostStatus;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

/**
 * Endpoint that reports the view of the cluster that this server has.
 * Returns every host that has been discovered by this server along side with information about its
 * status such as whether it is alive or dead and the last time its status got updated.
 */

@Path("/clusterStatus")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class ClusterStatusResource {

  private final KsqlEngine engine;
  private final HeartbeatAgent heartbeatAgent;

  public ClusterStatusResource(final KsqlEngine engine,final HeartbeatAgent heartbeatAgent) {
    this.engine = engine;
    this.heartbeatAgent = heartbeatAgent;
  }

  @GET
  public Response checkClusterStatus() {
    final ClusterStatusResponse response = getResponse();
    return Response.ok(response).build();
  }

  private ClusterStatusResponse getResponse() {
    Map<HostInfo, HostStatus> allHostStatus = heartbeatAgent.getHostsStatus();

    Map<HostInfoEntity, HostStatusEntity> response = allHostStatus
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> new HostInfoEntity(entry.getKey().host(), entry.getKey().port()) ,
            entry -> new HostStatusEntity(entry.getValue().isHostAlive(),
                                          entry.getValue().getLastStatusUpdateMs(),
                                          getActiveStandbyInformation(entry.getKey()))));

    return new ClusterStatusResponse(response);
  }

  private Map<String, ActiveStandbyEntity> getActiveStandbyInformation(HostInfo hostInfo) {
    final List<PersistentQueryMetadata> currentQueries = engine.getPersistentQueries();
    if (currentQueries.isEmpty()) {
      // empty response
      return Collections.emptyMap();
    }

    final Map<String, ActiveStandbyEntity> perQueryMap = new HashMap<>();
    for (PersistentQueryMetadata persistentMetadata: currentQueries) {
      for (StreamsMetadata streamsMetadata : persistentMetadata
          .getAllMetadata()) {
        if (streamsMetadata == null || !streamsMetadata.hostInfo().equals(hostInfo)) {
          continue;
        }
        if (streamsMetadata == StreamsMetadata.NOT_AVAILABLE) {
          continue;
        }
        final ActiveStandbyEntity entity = new ActiveStandbyEntity(
            streamsMetadata.stateStoreNames(),
            streamsMetadata.topicPartitions()
                .stream()
                .map(TopicPartition::toString)
                .collect(Collectors.toSet()),
            streamsMetadata.standbyStateStoreNames(),
            streamsMetadata.standbyTopicPartitions()
                .stream()
                .map(TopicPartition::toString)
                .collect(Collectors.toSet()));
        perQueryMap.put(((QueryMetadata) persistentMetadata).getQueryApplicationId(), entity);
      }
    }
    return perQueryMap;
  }
}
