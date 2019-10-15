/*
 * Copyright 2018 Confluent Inc.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PullQueryResponse {

  private static final Logger log = LoggerFactory.getLogger(PullQueryResponse.class);

  private final TransientQueryMetadata queryMetadata;
  private final long disconnectCheckInterval;
  private final ObjectMapper objectMapper;
  private volatile boolean limitReached = false;

  PullQueryResponse(
      final TransientQueryMetadata queryMetadata,
      final long disconnectCheckInterval,
      final ObjectMapper objectMapper
  ) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = Objects.requireNonNull(queryMetadata, "queryMetadata");
    this.queryMetadata.setLimitHandler(new LimitHandler());
    queryMetadata.start();
  }

  public String getResponse() {

    final List<GenericRow> rows = new ArrayList<>();
    try {
      while (queryMetadata.isRunning() && !limitReached) {
        final KeyValue<String, GenericRow> value = queryMetadata.getRowQueue().poll(
            disconnectCheckInterval,
            TimeUnit.MILLISECONDS
        );
        if (value != null) {
          rows.add(value.value);
        } else {
          limitReached = true;
        }

      }

      final ObjectMapper mapper = new ObjectMapper();
      final JsonNode rootNode = mapper.createObjectNode();

      for (GenericRow row : rows) {
        final JsonNode childNode = mapper.createObjectNode();
        ((ObjectNode) childNode).put("row", row.toString());

        ((ObjectNode) rootNode).set("data", childNode);
      }
      return  mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);

    } catch (final InterruptedException exception) {
      // The most likely cause of this is the server shutting down. Should just try to close
      // gracefully, without writing any more to the connection stream.
      log.warn("Interrupted while writing to connection stream");
    } catch (final Exception exception) {
      log.error("Exception occurred while writing to connection stream: ", exception);
    } finally {
      queryMetadata.close();
    }
    return null;
  }


  private class LimitHandler implements io.confluent.ksql.query.LimitHandler {
    @Override
    public void limitReached() {
      limitReached = true;
    }
  }
}
