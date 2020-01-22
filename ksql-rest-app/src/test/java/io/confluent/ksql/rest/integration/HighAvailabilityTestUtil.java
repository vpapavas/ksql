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
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import org.apache.kafka.streams.state.HostInfo;

class HighAvailabilityTestUtil {

  static void waitForClusterToBeDiscovered(final int numServers, final TestKsqlRestApp restApp) {
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

  static boolean allServersDiscovered(
      final int numServers,
      final Map<String, HostStatusEntity> clusterStatus) {

    if(clusterStatus.size() < numServers) {
      return false;
    }
    return true;
  }

  static void sendHeartbeartsEveryIntervalForWindowLength(
      final TestKsqlRestApp receiverApp,
      final HostInfo sender,
      final long interval,
      final long window) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < window) {
      sendHeartbeatRequest(receiverApp, sender, System.currentTimeMillis());
      try {
        Thread.sleep(interval);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  static ClusterStatusResponse  waitForRemoteServerToChangeStatus(
      final TestKsqlRestApp restApp,
      final HostInfo remoteServer,
      final BiFunction<HostInfo, Map<String, HostStatusEntity>, Boolean> function)
  {
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

  static boolean remoteServerIsDown(
      final HostInfo remoteServer,
      final Map<String, HostStatusEntity> clusterStatus) {
    if (!clusterStatus.containsKey(remoteServer.toString())) {
      return true;
    }
    for( Entry<String, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().contains(String.valueOf(remoteServer.port()))
          && !entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
  }

  static boolean remoteServerIsUp(
      final HostInfo remoteServer,
      final Map<String, HostStatusEntity> clusterStatus) {
    for( Entry<String, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().contains(String.valueOf(remoteServer.port()))
          && entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
  }

  static void sendHeartbeatRequest(
      final TestKsqlRestApp restApp,
      final HostInfo host,
      final long timestamp) {

    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {
      restClient.makeAsyncHeartbeatRequest(new HostInfoEntity(host.host(), host.port()), timestamp);
    }
  }

  static ClusterStatusResponse sendClusterStatusRequest(final TestKsqlRestApp restApp) {

    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {

      final RestResponse<ClusterStatusResponse> res = restClient.makeClusterStatusRequest();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }

      return res.getResponse();
    }
  }

}
