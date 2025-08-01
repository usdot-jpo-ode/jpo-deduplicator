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

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedSpatDeduplicatorTopology;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;
import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

public class ProcessedSpatDeduplicatorTopologyTest {

    String inputTopic = "topic.ProcessedSpat";
    String outputTopic = "topic.DeduplicatedProcessedSpat";

    ObjectMapper objectMapper;

    // Reference Message
    String inputProcessedSpat1;

    // Same as Processed SPaT 1
    String inputProcessedSpat2;

    // Time change of +1 minute
    String inputProcessedSpat3;

    // With a signal group removed
    String inputProcessedSpat4;

    // With a signal group's id changed
    String inputProcessedSpat5;

    // With a change of the light state of a signal group
    String inputProcessedSpat6;

    String key = "{\"rsuId\": \"10.164.6.16\", \"intersectionId\": 6311,\"region\": -1}";

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        objectMapper = DateJsonMapper.getInstance();

        // Load test files from resources

        // Reference Processed SPaT
        String processedSpatReference = new String(
                Files.readAllBytes(Paths
                        .get("src/test/resources/json/processed_spat/sample.processed_spat-reference.json")));
        ProcessedSpat processedSpatReferenceData = objectMapper.readValue(processedSpatReference, ProcessedSpat.class);

        // Reference Processed SPaT, Should be kept
        inputProcessedSpat1 = processedSpatReferenceData.toString();

        // Duplicate of Number 1, should be dropped
        inputProcessedSpat2 = processedSpatReferenceData.toString();

        // Time change of +1 minute, should be kept
        ProcessedSpat processedSpatIncreaseTime = objectMapper.readValue(processedSpatReferenceData.toString(),
                ProcessedSpat.class);
        // Convert ZonedDateTime to Instant, add 61 seconds, then convert back to
        // ZonedDateTime
        ZonedDateTime originalZdt = processedSpatIncreaseTime.getUtcTimeStamp();
        Instant newInstant = originalZdt.toInstant().plusSeconds(61);
        processedSpatIncreaseTime.setUtcTimeStamp(ZonedDateTime.ofInstant(newInstant, originalZdt.getZone()));
        inputProcessedSpat3 = processedSpatIncreaseTime.toString();

        // Signal group removed
        ProcessedSpat processedSpatSignalGroupRemoved = objectMapper.readValue(processedSpatReferenceData.toString(),
                ProcessedSpat.class);
        processedSpatSignalGroupRemoved.getStates().remove(0);
        inputProcessedSpat4 = processedSpatSignalGroupRemoved.toString();

        // Signal group ID changed
        ProcessedSpat processedSpatSignalGroupIdChanged = objectMapper.readValue(
                processedSpatSignalGroupRemoved.toString(),
                ProcessedSpat.class);
        processedSpatSignalGroupIdChanged.getStates().get(0).setSignalGroup(500);
        inputProcessedSpat5 = processedSpatSignalGroupIdChanged.toString();

        // Signal group light state changed
        ProcessedSpat processedSpatSignalGroupLightStateChanged = objectMapper.readValue(
                processedSpatSignalGroupIdChanged.toString(),
                ProcessedSpat.class);
        processedSpatSignalGroupLightStateChanged.getStates().get(0).getStateTimeSpeed().get(0)
                .setEventState(ProcessedMovementPhaseState.PROTECTED_CLEARANCE);
        inputProcessedSpat6 = processedSpatSignalGroupLightStateChanged.toString();
    }

    @Test
    public void testSerialization() throws JsonMappingException, JsonProcessingException {
        ProcessedSpat processedSpat = objectMapper.readValue(inputProcessedSpat1, ProcessedSpat.class);
        String json = processedSpat.toString();
        assertEquals(inputProcessedSpat1, json);
    }

    @Test
    public void testJsonSerdes() {
        Serde<ProcessedSpat> serdes = JsonSerdes.ProcessedSpat();
        ProcessedSpat deserialized = serdes.deserializer().deserialize(null, inputProcessedSpat1.getBytes());
        byte[] serialized = serdes.serializer().serialize(null, deserialized);
        assertThat(new String(serialized), jsonEquals(inputProcessedSpat1));
    }

    @Test
    public void testTopology() {
        props = new DeduplicatorProperties();
        props.setKafkaTopicProcessedSpat(inputTopic);
        props.setKafkaTopicDeduplicatedProcessedSpat(outputTopic);

        ProcessedSpatDeduplicatorTopology processedSpatDeduplicatorTopology = new ProcessedSpatDeduplicatorTopology(
                props);
        Topology topology = processedSpatDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<String, String> inputProcessedSpatData = driver.createInputTopic(
                    inputTopic,
                    Serdes.String().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<String, ProcessedSpat> outputProcessedSpatData = driver.createOutputTopic(
                    outputTopic,
                    Serdes.String().deserializer(),
                    JsonSerdes.ProcessedSpat().deserializer());

            inputProcessedSpatData.pipeInput(key, inputProcessedSpat1);
            inputProcessedSpatData.pipeInput(key, inputProcessedSpat2);
            inputProcessedSpatData.pipeInput(key, inputProcessedSpat3);
            inputProcessedSpatData.pipeInput(key, inputProcessedSpat4);
            inputProcessedSpatData.pipeInput(key, inputProcessedSpat5);
            inputProcessedSpatData.pipeInput(key, inputProcessedSpat6);

            List<KeyValue<String, ProcessedSpat>> processedSpatDeduplicatorResults = outputProcessedSpatData
                    .readKeyValuesToList();

            // validate that only 5 messages make it through
            assertEquals(5, processedSpatDeduplicatorResults.size());

            ProcessedSpat processedSpat1 = objectMapper.readValue(inputProcessedSpat1, ProcessedSpat.class);
            ProcessedSpat processedSpat3 = objectMapper.readValue(inputProcessedSpat3, ProcessedSpat.class);
            ProcessedSpat processedSpat4 = objectMapper.readValue(inputProcessedSpat4, ProcessedSpat.class);
            ProcessedSpat processedSpat5 = objectMapper.readValue(inputProcessedSpat5, ProcessedSpat.class);
            ProcessedSpat processedSpat6 = objectMapper.readValue(inputProcessedSpat6, ProcessedSpat.class);

            assertThat(processedSpatDeduplicatorResults.get(0).value.toString(), jsonEquals(processedSpat1.toString()));
            assertThat(processedSpatDeduplicatorResults.get(1).value.toString(), jsonEquals(processedSpat3.toString()));
            assertThat(processedSpatDeduplicatorResults.get(2).value.toString(), jsonEquals(processedSpat4.toString()));
            assertThat(processedSpatDeduplicatorResults.get(3).value.toString(), jsonEquals(processedSpat5.toString()));
            assertThat(processedSpatDeduplicatorResults.get(4).value.toString(), jsonEquals(processedSpat6.toString()));

        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
