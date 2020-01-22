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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HostStatusEntity {

  private boolean hostAlive;
  private long lastStatusUpdateMs;
  private Map<String, ActiveStandbyEntity> perQueryActiveStandbyEntity;

  @JsonCreator
  public HostStatusEntity(
      @JsonProperty("hostAlive") final boolean hostAlive,
      @JsonProperty("lastStatusUpdateMs") final long lastStatusUpdateMs,
      @JsonProperty("perQueryActiveStandbyEntity")
      final Map<String, ActiveStandbyEntity> perQueryActiveStandbyEntity
  ) {
    this.hostAlive = hostAlive;
    this.lastStatusUpdateMs = lastStatusUpdateMs;
    this.perQueryActiveStandbyEntity = perQueryActiveStandbyEntity;
  }

  public boolean getHostAlive() {
    return hostAlive;
  }

  public long getLastStatusUpdateMs() {
    return lastStatusUpdateMs;
  }

  public Map<String, ActiveStandbyEntity> getPerQueryActiveStandbyEntity() {
    return perQueryActiveStandbyEntity;
  }

  public void setHostAlive(final boolean hostAlive) {
    this.hostAlive = hostAlive;
  }

  public void setLastStatusUpdateMs(final long lastStatusUpdateMs) {
    this.lastStatusUpdateMs = lastStatusUpdateMs;
  }

  public void setPerQueryActiveStandbyEntity(final Map<String, ActiveStandbyEntity> perQueryActiveStandbyEntity) {
    this.perQueryActiveStandbyEntity = perQueryActiveStandbyEntity;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostStatusEntity that = (HostStatusEntity) o;
    return hostAlive == that.hostAlive
        && lastStatusUpdateMs == that.lastStatusUpdateMs
        && perQueryActiveStandbyEntity.equals(that.perQueryActiveStandbyEntity);

  }

  @Override
  public int hashCode() {
    return Objects.hash(hostAlive, lastStatusUpdateMs, perQueryActiveStandbyEntity);
  }

  @Override
  public String toString() {
    return hostAlive + "," + lastStatusUpdateMs + "," + perQueryActiveStandbyEntity;
  }
}
