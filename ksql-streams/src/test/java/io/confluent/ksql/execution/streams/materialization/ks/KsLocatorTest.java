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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsLocatorTest {

  private static final String STORE_NAME = "someStoreName";
  private static final URL LOCAL_HOST_URL = localHost();
  private static final Struct SOME_KEY = new Struct(SchemaBuilder.struct().build());

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KeyQueryMetadata keyQueryMetadata;
  @Mock
  private Serializer<Struct> keySerializer;
  @Mock
  private HostInfo activeHostInfo;
  @Mock
  private HostInfo standByHostInfo1;
  @Mock
  private HostInfo standByHostInfo2;

  private KsLocator locator;

  @Before
  public void setUp() {
    locator = new KsLocator(STORE_NAME, kafkaStreams, keySerializer, LOCAL_HOST_URL);

    when(activeHostInfo.host()).thenReturn("remoteHost");
    when(activeHostInfo.port()).thenReturn(2345);

    when(standByHostInfo1.host()).thenReturn("standBy1");
    when(standByHostInfo1.port()).thenReturn(1234);

    when(standByHostInfo2.host()).thenReturn("standBy2");
    when(standByHostInfo2.port()).thenReturn(5678);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(URL.class, LOCAL_HOST_URL)
        .setDefault(KafkaStreams.class, kafkaStreams)
        .setDefault(Serializer.class, keySerializer)
        .testConstructors(KsLocator.class, Visibility.PACKAGE);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void shouldRequestMetadata() {
    // Given:
    getEmtpyMetadata();

    // When:
    locator.locate(SOME_KEY, Optional.empty());

    // Then:
    verify(kafkaStreams).queryMetadataForKey(STORE_NAME, SOME_KEY, keySerializer);
  }

  @Test
  public void shouldReturnEmptyIfOwnerNotKnown() {
    // Given:
    getEmtpyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    assertThat(result.isEmpty(), is(true));
  }

  @Test
  public void shouldReturnOwnerIfKnown() {
    // Given:
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    final Optional<URI> url = result.stream().findFirst().map(KsqlNode::location);
    assertThat(url.map(URI::getScheme), is(Optional.of(LOCAL_HOST_URL.getProtocol())));
    assertThat(url.map(URI::getHost), is(Optional.of(activeHostInfo.host())));
    assertThat(url.map(URI::getPort), is(Optional.of(activeHostInfo.port())));
    assertThat(url.map(URI::getPath), is(Optional.of("/")));
  }

  @Test
  public void shouldReturnLocalOwnerIfSameAsSuppliedLocalHost() {
    // Given:
    when(activeHostInfo.host()).thenReturn(LOCAL_HOST_URL.getHost());
    when(activeHostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort());
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnLocalOwnerIfExplicitlyLocalHostOnSamePortAsSuppliedLocalHost() {
    // Given:
    when(activeHostInfo.host()).thenReturn("LocalHOST");
    when(activeHostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort());
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentHost() {
    // Given:
    when(activeHostInfo.host()).thenReturn("different");
    when(activeHostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort());
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPort() {
    // Given:
    when(activeHostInfo.host()).thenReturn(LOCAL_HOST_URL.getHost());
    when(activeHostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort() + 1);
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPortOnLocalHost() {
    // Given:
    when(activeHostInfo.host()).thenReturn("LOCALhost");
    when(activeHostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort() + 1);
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnStandBysWhenActiveDown() {
    // Given:
    getActiveAndStandbyMetadata();
    Map<String, HostInfo> hostStatus = ImmutableMap.of()

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, Optional.empty());

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private void givenOwnerMetadata(final Optional<HostInfo> hostInfo) {
    final KeyQueryMetadata metadata = hostInfo
        .map(hi -> {
          final KeyQueryMetadata md = mock(KeyQueryMetadata.class);
          when(md.getActiveHost()).thenReturn(hostInfo.get());
          return md;
        })
        .orElse(KeyQueryMetadata.NOT_AVAILABLE);

    when(kafkaStreams.queryMetadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(metadata);
  }

  @SuppressWarnings("unchecked")
  private void getEmtpyMetadata() {
    when(kafkaStreams.queryMetadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(KeyQueryMetadata.NOT_AVAILABLE);
  }

  @SuppressWarnings("unchecked")
  private void getActiveAndStandbyMetadata() {
    when(keyQueryMetadata.getActiveHost()).thenReturn(activeHostInfo);
    when(keyQueryMetadata.getStandbyHosts()).thenReturn(ImmutableSet.of(
        standByHostInfo1, standByHostInfo2));
    when(kafkaStreams.queryMetadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(keyQueryMetadata);
  }

  private static URL localHost() {
    try {
      return new URL("http://somehost:1234");
    } catch (final MalformedURLException e) {
      throw new AssertionError("Failed to build URL", e);
    }
  }
}