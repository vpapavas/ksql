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

package io.confluent.ksql.rest.server;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.streams.IRoutingFilter;
import io.confluent.ksql.rest.server.HeartbeatAgent.HostStatus;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.state.HostInfo;

public class LivenessFilter implements IRoutingFilter {

  private final Optional<HeartbeatAgent> heartbeatAgent;

  public LivenessFilter(final Optional<HeartbeatAgent> heartbeatAgent) {
    this.heartbeatAgent = requireNonNull(heartbeatAgent, "heartbeatAgent");
  }

  @Override
  public boolean filter(final HostInfo hostInfo, final String storeName, final int partition) {
    if (heartbeatAgent.isPresent()) {
      final Map<HostInfo, HostStatus> hostStatus = heartbeatAgent.get().getHostsStatus();
      if (!hostStatus.containsKey(hostInfo)) {
        return true;
      }
      System.out.println("-------------> Host " + hostInfo
                             + " is alive " + hostStatus.get(hostInfo).isHostAlive());
      return hostStatus.get(hostInfo).isHostAlive();
    }
    return true;
  }
}
