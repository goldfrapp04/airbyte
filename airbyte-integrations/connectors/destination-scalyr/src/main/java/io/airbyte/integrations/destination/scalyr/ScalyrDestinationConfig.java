package io.airbyte.integrations.destination.scalyr;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalyr.api.logs.EventAttributes;
import com.scalyr.api.logs.Events;
import javax.annotation.Nullable;

class ScalyrDestinationConfig {

  private final String logWriteApiKey;
  private final String logReadApiKey;
  private final int memoryLimit;
  private final String endpoint; // TODO only this is ever accessed twice
  private final EventAttributes serverAttributes;
  @Nullable private final Long overridingEventTs;

  private ScalyrDestinationConfig(final String logWriteApiKey, final String logReadApiKey, final int memoryLimit, final String endpoint, final EventAttributes serverAttributes,
      final Long overridingEventTs) {
    this.logWriteApiKey = logWriteApiKey;
    this.logReadApiKey = logReadApiKey;
    this.memoryLimit = memoryLimit;
    this.endpoint = endpoint;
    this.serverAttributes = serverAttributes;
    this.overridingEventTs = overridingEventTs;
  }

  static ScalyrDestinationConfig fromJson(final JsonNode config) {
    return new ScalyrDestinationConfig(
        config.get("scalyr_log_write_api_key").asText(),
        config.get("scalyr_log_read_api_key").asText(),
        config.get("scalyr_memory_limit_bytes") == null ? 4 * 1024 * 1024 : config.get("scalyr_memory_limit_bytes").asInt(), // TODO where did I find this default? Also, Events will NPE if passed a null, contrary to its javadoc
        config.get("scalyr_endpoint") == null ? "https://app.scalyr.com" : config.get("scalyr_endpoint").asText(),
        new EventAttributes("serverHost", "airbyte"), // TODO customize serverAttr. Try destination-gcs spec.json, search '"type": "object"'
        config.get("scalyr_event_timestamp_use_now") != null && config.get("scalyr_event_timestamp_use_now").asBoolean()
            ? System.currentTimeMillis() * 1_000_000
            : null);
  }

  static void initScalyr(final ScalyrDestinationConfig config) {
    Events.init(config.logWriteApiKey, config.memoryLimit, config.endpoint, config.serverAttributes);
  }

  String getLogReadApiKey() { return logReadApiKey; }
  String getEndpoint()      { return endpoint;      }
  @Nullable Long getOverridingEventTs() { return overridingEventTs; }
}
