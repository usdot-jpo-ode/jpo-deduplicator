package us.dot.its.jpo.deduplicator.deduplicator;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.serialization.Serde;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedMapDeduplicatorTopology;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;

import java.util.List;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;

public class ProcessedMapDeduplicatorTopologyTest {

    String inputTopic = "topic.ProcessedMap";
    String outputTopic = "topic.DeduplicatedProcessedMap";
    ObjectMapper objectMapper;
    TypeReference<ProcessedMap<LineString>> typeReference = new TypeReference<>() {
    };

    // Reference MAP
    String inputProcessedMap1 = "";

    // Duplicate of Number 1
    String inputProcessedMap2 = "";

    // A different Message entirely
    String inputProcessedMap3 = "";

    // Message 1 but 1 hour later
    String inputProcessedMap4 = "";

    String key = "{\"rsuId\":\"10.11.81.12\",\"intersectionId\":12109,\"region\":-1}";

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        objectMapper = DateJsonMapper.getInstance();

        // Load test files from resources
        // Reference MAP
        String processedMapReference = new String(Files
                .readAllBytes(Paths.get("src/test/resources/json/processed_map/sample.processed_map-reference.json")));
        ProcessedMap<LineString> processedMapReferenceData = objectMapper.readValue(processedMapReference, typeReference);

        inputProcessedMap1 = processedMapReferenceData.toString();

        // Duplicate of Number 1
        inputProcessedMap2 = processedMapReferenceData.toString();

        // Message 1 but 1 hour later
        ProcessedMap<LineString> processedMap1HourLater = objectMapper.readValue(processedMapReference, typeReference);
        ZonedDateTime originalZdt = processedMap1HourLater.getProperties().getOdeReceivedAt();
        Instant newInstant = originalZdt.toInstant().plusSeconds(3601);
        processedMap1HourLater.getProperties().setOdeReceivedAt(ZonedDateTime.ofInstant(newInstant, originalZdt.getZone()));
        inputProcessedMap3 = processedMap1HourLater.toString();
        
        // A different Message entirely
        String differentProcessedMapReference = new String(Files
                .readAllBytes(Paths.get("src/test/resources/json/processed_map/sample.processed_map-different.json")));
        ProcessedMap<LineString> processedMapDifferentData = objectMapper.readValue(differentProcessedMapReference, typeReference);
        inputProcessedMap4 = processedMapDifferentData.toString();
    }

    @Test
    public void testSerialization() throws JsonMappingException, JsonProcessingException {
        ProcessedMap<LineString> processedMap = objectMapper.readValue(inputProcessedMap1, typeReference);
        String json = processedMap.toString();
        assertEquals(inputProcessedMap1, json);
    }

    @Test
    public void testJsonSerdes() throws IOException {
        Serde<ProcessedMap<LineString>> serdes = JsonSerdes.ProcessedMapGeoJson();
        ProcessedMap<LineString> deserialized = serdes.deserializer().deserialize(null, inputProcessedMap1.getBytes());
        byte[] serialized = serdes.serializer().serialize(null, deserialized);
        String serializedString = new String(serialized);
        assertThat(serializedString, jsonEquals(inputProcessedMap1));
    }

    @Test
    public void testTopology() {

        props = new DeduplicatorProperties();
        props.setKafkaTopicProcessedMap(inputTopic);
        props.setKafkaTopicDeduplicatedProcessedMap(outputTopic);

        ProcessedMapDeduplicatorTopology processedMapDeduplicatorTopology = new ProcessedMapDeduplicatorTopology(props);

        Topology topology = processedMapDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<String, String> inputProcessedMapData = driver.createInputTopic(
                    inputTopic,
                    Serdes.String().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<String, ProcessedMap<LineString>> outputProcessedMapData = driver.createOutputTopic(
                    outputTopic,
                    Serdes.String().deserializer(),
                    JsonSerdes.ProcessedMapGeoJson().deserializer());

            inputProcessedMapData.pipeInput(key, inputProcessedMap1);
            inputProcessedMapData.pipeInput(key, inputProcessedMap2);
            inputProcessedMapData.pipeInput(key, inputProcessedMap3);
            inputProcessedMapData.pipeInput(key, inputProcessedMap4);

            List<KeyValue<String, ProcessedMap<LineString>>> mapDeduplicatorResults = outputProcessedMapData
                    .readKeyValuesToList();

            // validate that only 3 messages make it through
            assertEquals(3, mapDeduplicatorResults.size());

            ProcessedMap<LineString> map1 = objectMapper.readValue(inputProcessedMap1,
                    typeReference);
            ProcessedMap<LineString> map3 = objectMapper.readValue(inputProcessedMap3,
                    typeReference);
            ProcessedMap<LineString> map4 = objectMapper.readValue(inputProcessedMap4,
                    typeReference);

            assertEquals(map1.getProperties().getOdeReceivedAt(),
                    mapDeduplicatorResults.get(0).value.getProperties().getOdeReceivedAt());
            assertEquals(map3.getProperties().getOdeReceivedAt(),
                    mapDeduplicatorResults.get(1).value.getProperties().getOdeReceivedAt());
            assertEquals(map4.getProperties().getOdeReceivedAt(),
                    mapDeduplicatorResults.get(2).value.getProperties().getOdeReceivedAt());

        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
