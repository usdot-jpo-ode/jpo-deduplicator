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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.TimDeduplicatorTopology;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;


public class TimDeduplicatorTopologyTest {

    String inputTopic = "topic.OdeTimJson";
    String outputTopic = "topic.DeduplicatedOdeTimJson";

    ObjectMapper objectMapper;

    String inputTim1 = "";
    String inputTim2 = "";
    String inputTim3 = "";
    String inputTim4 = "";
    String inputTim5 = "";

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        objectMapper = DateJsonMapper.getInstance();

        // Load test files from resources
        // Reference TIM
        String timReference = new String(
                Files.readAllBytes(Paths.get("src/test/resources/json/ode_tim/sample.ode-tim-reference.json")));
        OdeMessageFrameData timReferenceData = objectMapper.readValue(timReference, OdeMessageFrameData.class);

        inputTim1 = timReferenceData.toJson();

        // Duplicate of Number 1 - should be deduplicated
        inputTim2 = timReferenceData.toJson();

        // Message 1 but 5 minutes later - should be deduplicated
        OdeMessageFrameData tim5MinutesLater = objectMapper.readValue(timReferenceData.toJson(),
                OdeMessageFrameData.class);
        String originalTime = timReferenceData.getMetadata().getOdeReceivedAt();
        Instant instant = Instant.parse(originalTime);
        Instant newInstant = instant.plus(5, ChronoUnit.MINUTES);
        tim5MinutesLater.getMetadata().setOdeReceivedAt(newInstant.toString());
        inputTim3 = tim5MinutesLater.toJson();

        // Message 1 but 1 hour later - should be kept
        OdeMessageFrameData tim1HourLater = objectMapper.readValue(timReferenceData.toJson(),
                OdeMessageFrameData.class);
        originalTime = timReferenceData.getMetadata().getOdeReceivedAt();
        instant = Instant.parse(originalTime);
        newInstant = instant.plus(61, ChronoUnit.MINUTES);
        tim1HourLater.getMetadata().setOdeReceivedAt(newInstant.toString());
        inputTim4 = tim1HourLater.toJson();

        // A different Message entirely - should be kept
        String timDifferent = new String(
                Files.readAllBytes(Paths.get("src/test/resources/json/ode_tim/sample.ode-tim-different.json")));
        OdeMessageFrameData timDifferentData = objectMapper.readValue(timDifferent, OdeMessageFrameData.class);
        inputTim5 = timDifferentData.toJson();
    }

    @Test
    public void testSerialization() throws JsonMappingException, JsonProcessingException {
        OdeMessageFrameData tim = objectMapper.readValue(inputTim1, OdeMessageFrameData.class);
        String json = objectMapper.writeValueAsString(tim);
        assertEquals(inputTim1, json);
    }

    @Test
    public void testJsonSerdes() {
        Serde<OdeMessageFrameData> serdes = JsonSerdes.OdeMessageFrame();
        OdeMessageFrameData deserialized = serdes.deserializer().deserialize(null, inputTim1.getBytes());
        byte[] serialized = serdes.serializer().serialize(null, deserialized);
        assertThat(new String(serialized), jsonEquals(inputTim1));
    }

    @Test
    public void testTopology() {

        props = new DeduplicatorProperties();
        props.setKafkaTopicOdeTimJson(inputTopic);
        props.setKafkaTopicDeduplicatedOdeTimJson(outputTopic);

        TimDeduplicatorTopology timDeduplicatorTopology = new TimDeduplicatorTopology(props);
        Topology topology = timDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<Void, String> inputTimData = driver.createInputTopic(
                inputTopic, 
                Serdes.Void().serializer(), 
                Serdes.String().serializer());

            TestOutputTopic<String, OdeMessageFrameData> outputTimData = driver.createOutputTopic(
                outputTopic, 
                Serdes.String().deserializer(), 
                JsonSerdes.OdeMessageFrame().deserializer());

            inputTimData.pipeInput(null, inputTim1);
            inputTimData.pipeInput(null, inputTim2);
            inputTimData.pipeInput(null, inputTim3);
            inputTimData.pipeInput(null, inputTim4);
            inputTimData.pipeInput(null, inputTim5);

            List<KeyValue<String, OdeMessageFrameData>> timDeduplicatedResults = outputTimData.readKeyValuesToList();

            // validate that only 3 messages make it through
            assertEquals(3, timDeduplicatedResults.size());

            OdeMessageFrameData tim1 = objectMapper.readValue(inputTim1, OdeMessageFrameData.class);
            OdeMessageFrameData tim4 = objectMapper.readValue(inputTim4, OdeMessageFrameData.class);
            OdeMessageFrameData tim5 = objectMapper.readValue(inputTim5, OdeMessageFrameData.class);

            assertEquals(tim1.getMetadata().getOdeReceivedAt(), timDeduplicatedResults.get(0).value.getMetadata().getOdeReceivedAt());
            assertEquals(tim4.getMetadata().getOdeReceivedAt(), timDeduplicatedResults.get(1).value.getMetadata().getOdeReceivedAt());
            assertEquals(tim5.getMetadata().getOdeReceivedAt(), timDeduplicatedResults.get(2).value.getMetadata().getOdeReceivedAt());

        }catch(Exception e){
            e.printStackTrace(); 
        }
    }
}
