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

package io.confluent.ksql.execution.streams.materialization.ks;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.streams.IRoutingFilter;
import io.confluent.ksql.execution.streams.materialization.Locator;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams implementation of {@link Locator}.
 */
final class KsLocator implements Locator {

  private static final Logger LOG = LoggerFactory.getLogger(KsLocator.class);
  private final String stateStoreName;
  private final KafkaStreams kafkaStreams;
  private final Serializer<Struct> keySerializer;
  private final URL localHost;

  KsLocator(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final Serializer<Struct> keySerializer,
      final URL localHost
  ) {
    this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    this.keySerializer = requireNonNull(keySerializer, "keySerializer");
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.localHost = requireNonNull(localHost, "localHost");
  }

  @Override
  public List<KsqlNode> locate(
      final Struct key,
      final List<IRoutingFilter> routingFilters
  ) {
    final KeyQueryMetadata metadata = kafkaStreams
        .queryMetadataForKey(stateStoreName, key, keySerializer);

    // FAil fast if Streams not ready. Let client handle it
    if (metadata == KeyQueryMetadata.NOT_AVAILABLE) {
      LOG.debug("Streams Metadata not available");
      return Collections.emptyList();
    }

    final HostInfo activeHost = metadata.getActiveHost();
    final Set<HostInfo> standByHosts = metadata.getStandbyHosts();

    LOG.debug("Before filtering: Active host {} , standby hosts {}", activeHost, standByHosts);

    List<HostInfo> hosts = new ArrayList<>();
    hosts.add(activeHost);
    hosts.addAll(standByHosts);

    // Filter out hosts based on liveness and lag filters.
    // The list is ordered by routing preference: active node is first, then standby nodes
    // in order of increasing lag.
    // If heartbeat is not enabled, all hosts are considered alive.
    List<KsqlNode> filteredHosts = new ArrayList<>();
    routingFilters
        .forEach(routingFilter -> hosts
            .stream()
            .filter(hostInfo -> routingFilter
                .filter(hostInfo, stateStoreName, metadata.getPartition()))
            .map(this::asNode)
            .collect(Collectors.toList())
        .addAll(filteredHosts));

    LOG.debug("Filtered and ordered hosts: {}", filteredHosts);
    return filteredHosts;
  }

  @VisibleForTesting
  KsqlNode asNode(final HostInfo hostInfo) {
    return new Node(
        isLocalHost(hostInfo),
        buildLocation(hostInfo)
    );
  }

  private boolean isLocalHost(final HostInfo hostInfo) {
    if (hostInfo.port() != localHost.getPort()) {
      return false;
    }

    return hostInfo.host().equalsIgnoreCase(localHost.getHost())
        || hostInfo.host().equalsIgnoreCase("localhost");
  }

  private URI buildLocation(final HostInfo remoteInfo) {
    try {
      return new URL(
          localHost.getProtocol(),
          remoteInfo.host(),
          remoteInfo.port(),
          "/"
      ).toURI();
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
          + " remoteInfo: " + remoteInfo);
    }
  }

  @Immutable
  private static final class Node implements KsqlNode {

    private final boolean local;
    private final URI location;
    private boolean isAlive;

    private Node(final boolean local, final URI location) {
      this.local = local;
      this.location = requireNonNull(location, "location");
      this.isAlive = false;
    }

    @Override
    public boolean isLocal() {
      return local;
    }

    @Override
    public URI location() {
      return location;
    }

    public boolean isAlive() {
      return isAlive;
    }

    public void setIsAlive(final boolean alive) {
      isAlive = alive;
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append(String.format("local = %s ,", local))
          .append(String.format("location = %s ,", location))
          .append(String.format("alive = %s .", isAlive))
          .toString();

    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Node that = (Node) o;
      return local == that.local
          && location.equals(that.location)
          && isAlive == that.isAlive;
    }

    @Override
    public int hashCode() {
      return Objects.hash(local, location, isAlive);
    }

  }
}
