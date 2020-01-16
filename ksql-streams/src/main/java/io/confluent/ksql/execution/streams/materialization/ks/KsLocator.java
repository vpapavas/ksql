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

import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.streams.materialization.Locator;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Kafka Streams implementation of {@link Locator}.
 */
final class KsLocator implements Locator {

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
  public List<KsqlNode> locate(final Struct key, Optional<Map<String, HostInfo>> hostStatuses) {
    final KeyQueryMetadata metadata = kafkaStreams
        .queryMetadataForKey(stateStoreName, key, keySerializer);

    if (metadata == KeyQueryMetadata.NOT_AVAILABLE) {
      return Collections.emptyList();
    }

    // TODO Do we need a timeout here when accessing metadata?
    ActiveAndStandByNodes activeAndStandByNodes = new ActiveAndStandByNodes();
    activeAndStandByNodes.setActive(asNode(metadata.getActiveHost()));
    metadata.getStandbyHosts()
        .stream()
        .map(this::asNode)
        .forEach(activeAndStandByNodes::addStandBy);



    final List<HostInfo> servingHosts = Lists.newArrayList();
    servingHosts.add(metadata.getActiveHost());
    servingHosts.addAll(metadata.getStandbyHosts());

    // filter out nodes that are dead
    if (hostStatuses.isPresent()) {
      // TODO fix after heartbeat merge
      servingHosts.stream().filter(hostInfo -> hostStatuses.get().containsKey(hostInfo));
    }
    return servingHosts.stream().map(this::asNode).collect(Collectors.toList());
  }

  private KsqlNode asNode(final HostInfo hostInfo) {
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

  class ActiveAndStandByNodes {
    private KsqlNode active;
    private final Set<KsqlNode> standBys;

    private ActiveAndStandByNodes() {
      standBys = new HashSet<>();
    }

    public KsqlNode getActive() {
      return active;
    }

    public Set<KsqlNode> getStandBys() {
      return standBys;
    }

    public void setActive(KsqlNode active) {
      this.active = active;
    }

    public void addStandBy(KsqlNode standBy) {
      this.standBys.add(standBy);
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

    public void setIsAlive(boolean alive) {
      isAlive = alive;
    }
  }
}
