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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.MapDeduplicatorTopology;
import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;
import us.dot.its.jpo.ode.model.OdeMapData;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class MapDeduplicatorTopologyTest {

    String inputTopic = "topic.OdeMapJson";
    String outputTopic = "topic.DeduplicatedOdeMapJson";
    ObjectMapper objectMapper;

    String inputMap1 = "";
    String inputMap2 = "";
    String inputMap3 = "";
    String inputMap4 = "";

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        // Load test files from resources
        
        // Reference MAP
        inputMap1 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/map/sample.map-reference.json")));

        // Duplicate of Number 1
        inputMap2 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/map/sample.map-reference.json")));

        // A different Message entirely
        inputMap3 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/map/sample.map-different.json")));

        // Message 1 but 1 hour later
        inputMap4 = new String(Files.readAllBytes(Paths.get("src/test/resources/json/map/sample.map-reference-1-hour-later.json")));
    }

    @Test
    public void testTopology() {

        props = new DeduplicatorProperties();
        props.setKafkaTopicOdeMapJson(inputTopic);
        props.setKafkaTopicDeduplicatedOdeMapJson(outputTopic);

        MapDeduplicatorTopology mapDeduplicatorTopology = new MapDeduplicatorTopology(props, null);

        Topology topology = mapDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            
            
            TestInputTopic<Void, String> inputOdeMapData = driver.createInputTopic(
                inputTopic, 
                Serdes.Void().serializer(), 
                Serdes.String().serializer());


            TestOutputTopic<String, OdeMapData> outputOdeMapData = driver.createOutputTopic(
                outputTopic, 
                Serdes.String().deserializer(), 
                JsonSerdes.OdeMap().deserializer());

            inputOdeMapData.pipeInput(null, inputMap1);
            inputOdeMapData.pipeInput(null, inputMap2);
            inputOdeMapData.pipeInput(null, inputMap3);
            inputOdeMapData.pipeInput(null, inputMap4);

            List<KeyValue<String, OdeMapData>> mapDeduplicationResults = outputOdeMapData.readKeyValuesToList();

            // validate that only 3 messages make it through
            assertEquals(3, mapDeduplicationResults.size());

            objectMapper = new ObjectMapper();
            OdeMapData map1 = objectMapper.readValue(inputMap1, OdeMapData.class);
            OdeMapData map3 = objectMapper.readValue(inputMap3, OdeMapData.class);
            OdeMapData map4 = objectMapper.readValue(inputMap4, OdeMapData.class);


            assertEquals(map1.getMetadata().getOdeReceivedAt(), mapDeduplicationResults.get(0).value.getMetadata().getOdeReceivedAt());
            assertEquals(map3.getMetadata().getOdeReceivedAt(), mapDeduplicationResults.get(1).value.getMetadata().getOdeReceivedAt());
            assertEquals(map4.getMetadata().getOdeReceivedAt(), mapDeduplicationResults.get(2).value.getMetadata().getOdeReceivedAt());
           
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}