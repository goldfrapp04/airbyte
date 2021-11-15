/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.scalyr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalyr.api.query.QueryService;
import com.scalyr.api.query.QueryService.PageMode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScalyrDestinationAcceptanceTest extends DestinationAcceptanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScalyrDestinationAcceptanceTest.class);

  private final ObjectMapper mapper = new ObjectMapper();

  private JsonNode configJson;
  private ScalyrDestinationConfig config;
  private QueryService querySvc;

  @Override
  protected String getImageName() {
    return "airbyte/destination-scalyr:dev";
  }

  @Override
  protected JsonNode getConfig() {
    return configJson;
  }

  @Override
  protected JsonNode getFailCheckConfig() {
    final var badConfig = Jsons.clone(configJson);
    ((ObjectNode) badConfig).put("scalyr_endpoint", "https://i-am-not-scalyr.com/");
    return badConfig;
  }

  @Override
  protected List<JsonNode> retrieveRecords(final TestDestinationEnv testEnv,
                                           final String streamName,
                                           final String namespace,
                                           final JsonNode streamSchema)
      throws IOException {
    final String filter = String.format("serverHost = 'airbyte' nonce = 'nonce_%d'", ScalyrConsumer.SCALYR_OVERRIDING_EVENT_TS);
    LOGGER.info("Querying with filter \"" + filter + "\"");

    QueryService.LogQueryResult result;
    final Instant startTime = Instant.now();
    do { // TODO don't want Airbyte heavy-hitting Scalyr either
      result = querySvc.logQuery(filter, "10 minutes", "0 minutes", 200, PageMode.head, null, null);
    } while (result.matches.size() < 5
        && Duration.between(startTime, Instant.now()).toSeconds() < 10);

    return result.matches.stream()
        .map(match -> {
          final ObjectNode node = mapper.createObjectNode();
          match.fields.getEntries().forEach(e -> node.set(
              e.getKey(),
              mapper.convertValue(e.getValue(), JsonNode.class)));
          return node;
        })
        .collect(Collectors.toList());
  }

  @Override
  protected void setup(final TestDestinationEnv testEnv) {
    this.configJson = Jsons.deserialize(IOs.readFile(Path.of("/Users/shia/code/airbyte/secrets/config.json")));
    this.config     = ScalyrDestinationConfig.fromJson(this.configJson);
    this.querySvc   = new QueryService(this.config.getLogReadApiKey());
  }

  @Override
  protected void tearDown(final TestDestinationEnv testEnv) {
  }

}
