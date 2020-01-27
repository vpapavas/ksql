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

package io.confluent.ksql.rest.integration;

import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

class HighAvailabilityTestUtil {

  static ClusterStatusResponse sendClusterStatusRequest(final TestKsqlRestApp restApp) {

    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {

      final RestResponse<ClusterStatusResponse> res = restClient.makeClusterStatusRequest();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }

      return res.getResponse();
    }
  }

  static void sendHeartbeartsForWindowLength(
      final TestKsqlRestApp receiverApp,
      final HostInfoEntity sender,
      final long window
  ) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < window) {
      sendHeartbeatRequest(receiverApp, sender, System.currentTimeMillis());
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  static ClusterStatusResponse  waitForRemoteServerToChangeStatus(
      final TestKsqlRestApp restApp,
      final HostInfoEntity remoteServer,
      final BiFunction<HostInfoEntity, Map<HostInfoEntity, HostStatusEntity>, Boolean> function
  ) {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(restApp);
      if(function.apply(remoteServer, clusterStatusResponse.getClusterStatus())) {
        return clusterStatusResponse;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  static void waitForClusterToBeDiscovered(
      final TestKsqlRestApp restApp, final int numServers
  ) {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(restApp);
      if(allServersDiscovered(numServers, clusterStatusResponse.getClusterStatus())) {
        break;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  static void waitForStreamsMetadataToInitialize(
      final TestKsqlRestApp restApp, List<HostInfoEntity> hosts, String queryId
  ) {

    while (true) {
      ClusterStatusResponse clusterStatusResponse = HighAvailabilityTestUtil.sendClusterStatusRequest(restApp);
      List<HostInfoEntity> initialized = hosts.stream().filter(
          hostInfo -> clusterStatusResponse
              .getClusterStatus()
              .get(hostInfo)
              .getPerQueryActiveStandbyEntity()
              .get(queryId) != null).collect(Collectors.toList());
      if(initialized.size() == hosts.size())
        break;
    }
    try {
      Thread.sleep(200);
    } catch (final Exception e) {
      // Meh
    }
  }

  static boolean remoteServerIsDown(
      final HostInfoEntity remoteServer,
      final Map<HostInfoEntity, HostStatusEntity> clusterStatus
  ) {
    if (!clusterStatus.containsKey(remoteServer)) {
      return true;
    }
    for( Entry<HostInfoEntity, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().getPort() == remoteServer.getPort()
          && !entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
  }

  static boolean remoteServerIsUp(
      final HostInfoEntity remoteServer,
      final Map<HostInfoEntity, HostStatusEntity> clusterStatus
  ) {
    for( Entry<HostInfoEntity, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().getPort() == remoteServer.getPort()
          && entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
  }

  private static boolean allServersDiscovered(
      final int numServers,
      final Map<HostInfoEntity, HostStatusEntity> clusterStatus
  ) {

    return clusterStatus.size() >= numServers;
  }

  private static void sendHeartbeatRequest(
      final TestKsqlRestApp restApp,
      final HostInfoEntity hostInfoEntity,
      final long timestamp
  ) {

    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {
      restClient.makeAsyncHeartbeatRequest(hostInfoEntity, timestamp);
    }
  }
}
