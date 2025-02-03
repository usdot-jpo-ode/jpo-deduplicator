package deduplicator;

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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedMapWktDeduplicatorTopology;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.List;

public class ProcessedMapWktDeduplicatorTopologyTest {

    String inputTopic = "topic.ProcessedMapWKT";
    String outputTopic = "topic.DeduplicatedProcessedMapWKT";
    


    TypeReference<ProcessedMap<String>> typeReference = new TypeReference<>(){};
    ObjectMapper objectMapper = new ObjectMapper();

    // Reference MAP
    String inputProcessedMapWkt1 = "";

    // Duplicate of Number 1
    String inputProcessedMapWkt2 = "";

    // A different Message entirely
    String inputProcessedMapWkt3 = "";

    // Message 1 but 1 hour later
    String inputProcessedMapWkt4 = "";

    String key = "{\"rsuId\":\"10.11.81.12\",\"intersectionId\":12109,\"region\":-1}";

    @Autowired
    DeduplicatorProperties props;


    @Before
    public void setup() throws IOException {
        // Load test files from resources
        
        // Reference MAP
        inputProcessedMapWkt1 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_map_wkt/sample.processed_map_wkt-reference.json")));

        // Duplicate of Number 1
        inputProcessedMapWkt2 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_map_wkt/sample.processed_map_wkt-reference.json")));

        // A different Message entirely
        inputProcessedMapWkt3 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_map_wkt/sample.processed_map_wkt-different.json")));

        // Message 1 but 1 hour later
        inputProcessedMapWkt4 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_map_wkt/sample.processed_map_wkt-reference-1-hour-later.json")));
    }
    

    @Test
    public void testTopology() {

        props = new DeduplicatorProperties();
        props.setKafkaTopicProcessedMapWKT(inputTopic);
        props.setKafkaTopicDeduplicatedProcessedMapWKT(outputTopic);

        ProcessedMapWktDeduplicatorTopology processedMapDeduplicatorTopology = new ProcessedMapWktDeduplicatorTopology(props, null);

        Topology topology = processedMapDeduplicatorTopology.buildTopology();
        objectMapper.registerModule(new JavaTimeModule());
        
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            
            
            TestInputTopic<String, String> inputProcessedMapData = driver.createInputTopic(
                inputTopic, 
                Serdes.String().serializer(), 
                Serdes.String().serializer());


            TestOutputTopic<String, ProcessedMap<String>> outputProcessedMapData = driver.createOutputTopic(
                outputTopic, 
                Serdes.String().deserializer(), 
                JsonSerdes.ProcessedMapWKT().deserializer());

            inputProcessedMapData.pipeInput(key, inputProcessedMapWkt1);
            inputProcessedMapData.pipeInput(key, inputProcessedMapWkt2);
            inputProcessedMapData.pipeInput(key, inputProcessedMapWkt3);
            inputProcessedMapData.pipeInput(key, inputProcessedMapWkt4);

            List<KeyValue<String, ProcessedMap<String>>> mapDeduplicatorResults = outputProcessedMapData.readKeyValuesToList();

            // validate that only 3 messages make it through
            assertEquals(3, mapDeduplicatorResults.size());

            ProcessedMap<String> map1 = objectMapper.readValue(inputProcessedMapWkt1, typeReference);
            ProcessedMap<String> map2 = objectMapper.readValue(inputProcessedMapWkt2, typeReference);
            ProcessedMap<String> map4 = objectMapper.readValue(inputProcessedMapWkt4, typeReference);

            assertEquals(map1.getProperties().getOdeReceivedAt(), mapDeduplicatorResults.get(0).value.getProperties().getOdeReceivedAt());
            assertEquals(map2.getProperties().getOdeReceivedAt(), mapDeduplicatorResults.get(1).value.getProperties().getOdeReceivedAt());
            assertEquals(map4.getProperties().getOdeReceivedAt(), mapDeduplicatorResults.get(2).value.getProperties().getOdeReceivedAt());
           
            

        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
