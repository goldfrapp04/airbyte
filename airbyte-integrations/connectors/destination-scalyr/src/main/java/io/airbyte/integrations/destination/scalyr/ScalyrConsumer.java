package io.airbyte.integrations.destination.scalyr;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.scalyr.api.logs.EventAttributes;
import com.scalyr.api.logs.Events;
import com.scalyr.api.logs.Severity;
import io.airbyte.integrations.base.FailureTrackingAirbyteMessageConsumer;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScalyrConsumer extends FailureTrackingAirbyteMessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScalyrConsumer.class);
  @VisibleForTesting
  static final long SCALYR_OVERRIDING_EVENT_TS = System.currentTimeMillis() * 1_000_000;

  private final ScalyrDestinationConfig config;
  private final ConfiguredAirbyteCatalog configuredCatalog;
  private final Consumer<AirbyteMessage> outputRecordCollector;
//  private final Map<AirbyteStreamNameNamespacePair, ScalyrWriter> writers;

  private AirbyteMessage lastStateMessage = null;

  ScalyrConsumer(final ScalyrDestinationConfig config, final ConfiguredAirbyteCatalog configuredCatalog, final Consumer<AirbyteMessage> outputRecordCollector) {
    this.config = config;
    this.configuredCatalog = configuredCatalog;
    this.outputRecordCollector = outputRecordCollector;
//    this.writers = new HashMap<>(configuredCatalog.getStreams().size());

    if (config.getOverridingEventTs() != null)
      LOGGER.info("Uploading event with ts " + config.getOverridingEventTs());
  }

  @Override
  protected void startTracked() throws Exception {
    ScalyrDestinationConfig.initScalyr(config);

//    for (ConfiguredAirbyteStream stream : configuredCatalog.getStreams()) {
//      if (stream.getSyncMode() != SyncMode.INCREMENTAL) // TODO my catalog has null, fix and resurrect the assert
//        throw new IllegalArgumentException("TODO");
//      writers.put(AirbyteStreamNameNamespacePair.fromAirbyteSteam(stream.getStream()), new ScalyrWriter());
//    }
  }

  @Override
  protected void acceptTracked(final AirbyteMessage msg) throws Exception {
//    LOGGER.info("Got msg " + msg);
    if (msg.getType() == Type.STATE) {
      lastStateMessage = msg;
      return;
    } else if (msg.getType() != Type.RECORD) {
      LOGGER.error("Unrecognized type " + msg.getType());
      return;
    }

    final var               record = msg.getRecord();
    final var            objMapper = new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    final Map<String, Object> data = objMapper.convertValue(record.getData(), new TypeReference<>() {});

    // Events are uploaded to the Scalyr servers after a short buffering delay, on the order of 5 seconds
    Events.event(getSeverity(data), getEventAttributes(data), getTimestamp(record.getEmittedAt(), data));
//    writers.get(AirbyteStreamNameNamespacePair.fromRecordMessage(record)) // TODO what to do?
  }

  @Override
  protected void close(final boolean hasFailed) throws Exception {
    if (hasFailed) {
      LOGGER.error("ScalyrConsumer has failed. Not flushing remaining buffered events");
    } else {
      LOGGER.info("Flushing remaining buffered events");
      Events.flush(); // ensure that all events are uploaded
      outputRecordCollector.accept(lastStateMessage);
    }
  }

  /** severity > sev. Default info. */
  private static Severity getSeverity(final Map<String, Object> data) {
    final int sevInt = data.containsKey("severity")
        ? parseInt(data, "severity", Severity.info.ordinal())
        : (data.containsKey("sev")
            ? parseInt(data, "sev", Severity.info.ordinal())
            : Severity.info.ordinal());

    switch (sevInt) {
      case 0: return Severity.finest;
      case 1: return Severity.finer;
      case 2: return Severity.fine;
      case 3: return Severity.info;
      case 4: return Severity.warning;
      case 5: return Severity.error;
      case 6: return Severity.fatal;
      default:
        LOGGER.error(String.format("Unexpected severity %d. Will use %d", sevInt, Severity.error.ordinal()));
        return Severity.error;
    }
  }

  private EventAttributes getEventAttributes(final Map<String, Object> data) {
    final var attrs = new EventAttributes(data.entrySet().stream()
        .filter(entry -> !("severity".equals(entry.getKey()) || "sev".equals(entry.getKey())
                        || "timestamp".equals(entry.getKey()) || "ts".equals(entry.getKey())))
        .collect(Collectors.toList())); // TODO flatten json?
    if (config.getOverridingEventTs() != null)
      attrs.put("nonce", "nonce_" + SCALYR_OVERRIDING_EVENT_TS);
    LOGGER.info("Will upload " + attrs);
    return attrs;
  }

  /** timestamp > ts. Nanoseconds. Default to record.getEmittedAt(). */
  private long getTimestamp(final long recordEmittedAt, final Map<String, Object> data) {
    if (config.getOverridingEventTs() != null)
      return SCALYR_OVERRIDING_EVENT_TS;

    return data.containsKey("timestamp")
        ? parseLong(data, "timestamp", recordEmittedAt * 1_000_000) // TODO how to verify it's nanos?
        : (data.containsKey("ts")
            ? parseLong(data, "ts", recordEmittedAt * 1_000_000)
            : (recordEmittedAt * 1_000_000));
  }

  private static int parseInt(final Map<String, Object> data, final String field, final int defaultVal) { // TODO String ... fields
    try {
      return Integer.parseInt((String) data.get(field));
    } catch (final NumberFormatException e) {
      LOGGER.error(String.format("Error parsing %s %s. Will use %d", field, data.get(field), defaultVal), e);
      return defaultVal;
    }
  }

  private static long parseLong(final Map<String, Object> data, final String field, final long defaultVal) {
    try {
      return Long.parseLong((String) data.get(field));
    } catch (final NumberFormatException e) {
      LOGGER.error(String.format("Error parsing %s %s. Will use %d", field, data.get(field), defaultVal), e);
      return defaultVal;
    }
  }
}
