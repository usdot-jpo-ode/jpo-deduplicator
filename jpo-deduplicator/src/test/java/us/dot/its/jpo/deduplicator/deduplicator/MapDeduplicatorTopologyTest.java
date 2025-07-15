package us.dot.its.jpo.deduplicator.deduplicator;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.MapDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class MapDeduplicatorTopologyTest {

    String inputTopic = "topic.OdeMapJson";
    String outputTopic = "topic.DeduplicatedOdeMapJson";
    ObjectMapper objectMapper;

    String inputMap1 = "";
    String inputMap2 = "";
    String inputMap4 = "";
    String inputMap5 = "";
    String inputMap3 = "";

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        objectMapper = new ObjectMapper();

        // Load test files from resources
        // Reference MAP
        String mapReference = new String(
                Files.readAllBytes(Paths.get("src/test/resources/json/ode_map/sample.ode-map-reference.json")));
        OdeMessageFrameData mapReferenceData = objectMapper.readValue(mapReference, OdeMessageFrameData.class);

        inputMap1 = mapReferenceData.toJson();

        // Duplicate of Number 1 - should be deduplicated
        inputMap2 = mapReferenceData.toJson();

        // Message 1 but 5 minutes later - should be deduplicated
        OdeMessageFrameData map5MinutesLater = objectMapper.readValue(mapReferenceData.toJson(),
                OdeMessageFrameData.class);
        String originalTime = mapReferenceData.getMetadata().getOdeReceivedAt();
        Instant instant = Instant.parse(originalTime);
        Instant newInstant = instant.plus(5, ChronoUnit.MINUTES);
        map5MinutesLater.getMetadata().setOdeReceivedAt(newInstant.toString());
        inputMap3 = map5MinutesLater.toJson();

        // A different Message entirely - should be kept
        String mapDifferent = new String(
                Files.readAllBytes(Paths.get("src/test/resources/json/ode_map/sample.ode-map-different.json")));
        OdeMessageFrameData mapDifferentData = objectMapper.readValue(mapDifferent, OdeMessageFrameData.class);
        inputMap4 = mapDifferentData.toJson();

        // Message 1 but 1 hour later - should be kept
        OdeMessageFrameData map1HourLater = objectMapper.readValue(mapReferenceData.toJson(),
                OdeMessageFrameData.class);
        originalTime = map1HourLater.getMetadata().getOdeReceivedAt();
        instant = Instant.parse(originalTime);
        newInstant = instant.plus(61, ChronoUnit.HOURS);
        map1HourLater.getMetadata().setOdeReceivedAt(newInstant.toString());
        inputMap5 = map1HourLater.toJson();
    }

    @Test
    public void testTopology() {

        props = new DeduplicatorProperties();
        props.setKafkaTopicOdeMapJson(inputTopic);
        props.setKafkaTopicDeduplicatedOdeMapJson(outputTopic);

        MapDeduplicatorTopology mapDeduplicatorTopology = new MapDeduplicatorTopology(props);

        Topology topology = mapDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<Void, String> inputOdeMapData = driver.createInputTopic(
                    inputTopic,
                    Serdes.Void().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<String, OdeMessageFrameData> outputOdeMapData = driver.createOutputTopic(
                    outputTopic,
                    Serdes.String().deserializer(),
                    JsonSerdes.OdeMessageFrame().deserializer());

            inputOdeMapData.pipeInput(null, inputMap1);
            inputOdeMapData.pipeInput(null, inputMap2);
            inputOdeMapData.pipeInput(null, inputMap4);
            inputOdeMapData.pipeInput(null, inputMap5);
            inputOdeMapData.pipeInput(null, inputMap3);

            List<KeyValue<String, OdeMessageFrameData>> mapDeduplicationResults = outputOdeMapData
                    .readKeyValuesToList();

            // validate that only 3 messages make it through
            assertEquals(3, mapDeduplicationResults.size());

            objectMapper = new ObjectMapper();
            OdeMessageFrameData map1 = objectMapper.readValue(inputMap1, OdeMessageFrameData.class);
            OdeMessageFrameData map3 = objectMapper.readValue(inputMap4, OdeMessageFrameData.class);
            OdeMessageFrameData map4 = objectMapper.readValue(inputMap5, OdeMessageFrameData.class);

            assertThat(mapDeduplicationResults.get(0).value.toJson(), jsonEquals(map1.toJson()));
            assertThat(mapDeduplicationResults.get(1).value.toJson(), jsonEquals(map3.toJson()));
            assertThat(mapDeduplicationResults.get(2).value.toJson(), jsonEquals(map4.toJson()));

        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}