package us.dot.its.jpo.deduplicator.deduplicator;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.TimDeduplicatorTopology;
import us.dot.its.jpo.ode.model.OdeTimData;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class TimDeduplicatorTopologyTest {

    String inputTopic = "topic.OdeTimJson";
    String outputTopic = "topic.DeduplicatedOdeTimJson";

    ObjectMapper objectMapper = new ObjectMapper();

    String inputOdeTimReference = "";
    String inputOdeTimOneSecondTimeDelta = "";
    String inputOdeTimOneHourTimeDelta = "";
    String inputOdeTimDifferent = "";

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        inputOdeTimReference = new String(Files.readAllBytes(Paths.get("src/test/resources/json/ode_tim/sample.ode-tim-reference.json")));
        inputOdeTimOneSecondTimeDelta = new String(Files.readAllBytes(Paths.get("src/test/resources/json/ode_tim/sample.ode-tim-reference-1-second-later.json")));
        inputOdeTimOneHourTimeDelta = new String(Files.readAllBytes(Paths.get("src/test/resources/json/ode_tim/sample.ode-tim-reference-1-hour-later.json")));
        inputOdeTimDifferent = new String(Files.readAllBytes(Paths.get("src/test/resources/json/ode_tim/sample.ode-tim-different.json")));
    }

    @Test
    public void testTopology() {

        props = new DeduplicatorProperties();
        props.setKafkaTopicOdeTimJson(inputTopic);
        props.setKafkaTopicDeduplicatedOdeTimJson(outputTopic);

        TimDeduplicatorTopology TimDeduplicatorTopology = new TimDeduplicatorTopology(props, null);

        Topology topology = TimDeduplicatorTopology.buildTopology();
        objectMapper.registerModule(new JavaTimeModule());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            
            
            TestInputTopic<Void, String> inputTimData = driver.createInputTopic(
                inputTopic, 
                Serdes.Void().serializer(), 
                Serdes.String().serializer());


            TestOutputTopic<String, OdeTimData> outputTimData = driver.createOutputTopic(
                outputTopic, 
                Serdes.String().deserializer(), 
                JsonSerdes.OdeTim().deserializer());

            inputTimData.pipeInput(null, inputOdeTimReference);
            inputTimData.pipeInput(null, inputOdeTimOneSecondTimeDelta);
            inputTimData.pipeInput(null, inputOdeTimOneHourTimeDelta);
            inputTimData.pipeInput(null, inputOdeTimDifferent);

            List<KeyValue<String, OdeTimData>> timDeduplicatedResults = outputTimData.readKeyValuesToList();

            // validate that only 3 messages make it through
            assertEquals(3, timDeduplicatedResults.size());

            objectMapper = new ObjectMapper();
            OdeTimData inputOdeTimReferenceObj = objectMapper.readValue(inputOdeTimReference, OdeTimData.class);
            OdeTimData inputOdeTimOneHourTimeDeltaObj = objectMapper.readValue(inputOdeTimOneHourTimeDelta, OdeTimData.class);
            OdeTimData inputOdeTimDifferentObj = objectMapper.readValue(inputOdeTimDifferent, OdeTimData.class);

            assertEquals(inputOdeTimReferenceObj.getMetadata().getOdeReceivedAt(), timDeduplicatedResults.get(0).value.getMetadata().getOdeReceivedAt());
            assertEquals(inputOdeTimOneHourTimeDeltaObj.getMetadata().getOdeReceivedAt(), timDeduplicatedResults.get(1).value.getMetadata().getOdeReceivedAt());
            assertEquals(inputOdeTimDifferentObj.getMetadata().getOdeReceivedAt(), timDeduplicatedResults.get(2).value.getMetadata().getOdeReceivedAt());
        
        }catch(Exception e){
            e.printStackTrace(); 
        }
    }
}
