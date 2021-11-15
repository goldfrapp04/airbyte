/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.scalyr;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalyr.api.logs.EventAttributes;
import com.scalyr.api.logs.Events;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScalyrDestination extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScalyrDestination.class);

  public static void main(final String[] args) throws Exception {
    new IntegrationRunner(new ScalyrDestination()).run(args);
  }

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    try {
      final ScalyrDestinationConfig sdc = ScalyrDestinationConfig.fromJson(config);
      ScalyrDestinationConfig.initScalyr(sdc);
      Events.info(new EventAttributes("tag", "airbyte", "op", "check"));
      Events.flush();
      return new AirbyteConnectionStatus()
          .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
          .withMessage("Successfully uploaded test event. Try searching \"tag='airbyte-test'\" in " + sdc.getEndpoint());
    } catch (final Exception e) {
      LOGGER.error("Exception when accessing Scalyr", e);
      return new AirbyteConnectionStatus()
          .withStatus(AirbyteConnectionStatus.Status.FAILED)
          .withMessage(String.format("Could not access Scalyr with provided configurations %s. Error: %s", config, e));
    }
  }

  @Override
  public AirbyteMessageConsumer getConsumer(final JsonNode config,
                                            final ConfiguredAirbyteCatalog configuredCatalog,
                                            final Consumer<AirbyteMessage> outputRecordCollector) {
    return new ScalyrConsumer(ScalyrDestinationConfig.fromJson(config), configuredCatalog, outputRecordCollector);
  }
}
