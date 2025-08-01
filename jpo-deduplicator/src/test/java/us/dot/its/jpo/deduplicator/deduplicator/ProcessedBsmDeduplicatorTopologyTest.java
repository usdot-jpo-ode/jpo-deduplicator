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
import com.fasterxml.jackson.core.type.TypeReference;

import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedBsmDeduplicatorTopology;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

public class ProcessedBsmDeduplicatorTopologyTest {

    String inputTopic = "topic.ProcessedBsmJson";
    String outputTopic = "topic.DeduplicatedOdeBsmJson";
    ObjectMapper objectMapper;

    String inputProcessedBsm1 = "";

    // Same as BSM 1 - No Message should be generated
    String inputProcessedBsm2 = "";

    // Increase Time from BSM 1
    String inputProcessedBsm3 = "";

    // Vehicle Speed not 0
    String inputProcessedBsm4 = "";

    // Vehicle Position has changed
    String inputProcessedBsm5 = "";

    // Vehicle Position was removed
    String inputProcessedBsm6 = "";

    // Vehicle Position was added back in
    String inputProcessedBsm7 = "";

    // Vehicle Speed was removed
    String inputProcessedBsm8 = "";

    String key = "{\"rsuId\":\"172.19.0.1\",\"logId\":\"1234567890\",\"bsmId\":\"31325433\"}";

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        objectMapper = DateJsonMapper.getInstance();

        // Load test files from resources
        // Reference Processed BSM
        String processedBsmReference = new String(
                Files.readAllBytes(Paths
                        .get("src/test/resources/json/processed_bsm/sample.processed_bsm-reference.json")));
        ProcessedBsm<Point> processedBsmReferenceData = objectMapper.readValue(processedBsmReference,
                new TypeReference<ProcessedBsm<Point>>() {
                });

        // Reference Processed BSM, Should be forwarded
        inputProcessedBsm1 = processedBsmReferenceData.toString();

        // Duplicate of Number 1, should be dropped
        inputProcessedBsm2 = processedBsmReferenceData.toString();

        // Increase Time from Bsm 1, Should be forwarded
        ProcessedBsm<Point> processedBsmIncreaseTime = objectMapper.readValue(processedBsmReferenceData.toString(),
                new TypeReference<ProcessedBsm<Point>>() {
                });
        // Convert ZonedDateTime to Instant, add 15 seconds, then convert back
        ZonedDateTime originalZdt = processedBsmIncreaseTime.getProperties().getTimeStamp();
        Instant newInstant = originalZdt.toInstant().plusSeconds(15);
        processedBsmIncreaseTime.getProperties()
                .setTimeStamp(ZonedDateTime.ofInstant(newInstant, originalZdt.getZone()));
        inputProcessedBsm3 = processedBsmIncreaseTime.toString();

        // Vehicle Speed not 0, Should be Forwarded
        ProcessedBsm<Point> processedBsmSpeedNotZero = objectMapper.readValue(processedBsmReferenceData.toString(),
                new TypeReference<ProcessedBsm<Point>>() {
                });
        processedBsmSpeedNotZero.getProperties().setSpeed(10.0);
        inputProcessedBsm4 = processedBsmSpeedNotZero.toString();

        // Vehicle Position has changed, Should be Forwarded
        ProcessedBsm<Point> processedBsmPositionChanged = new ProcessedBsm<Point>(null,
                new Point(new double[] { -105.0, 50.0 }), processedBsmReferenceData.getProperties());
        inputProcessedBsm5 = processedBsmPositionChanged.toString();

        // Vehicle Position was removed. Should be Forwarded
        ProcessedBsm<Point> processedBsmPositionRemoved = new ProcessedBsm<Point>(null,
                null, processedBsmReferenceData.getProperties());
        inputProcessedBsm6 = processedBsmPositionRemoved.toString();

        // Vehicle Position was added back in. Should be Forwarded
        ProcessedBsm<Point> processedBsmPositionAddedBack = objectMapper.readValue(processedBsmReferenceData.toString(),
                new TypeReference<ProcessedBsm<Point>>() {
                });
        inputProcessedBsm7 = processedBsmPositionAddedBack.toString();

        // Vehicle is missing its speed section, Should be Forwarded
        ProcessedBsm<Point> processedBsmSpeedMissing = objectMapper.readValue(processedBsmReferenceData.toString(),
                new TypeReference<ProcessedBsm<Point>>() {
                });
        processedBsmSpeedMissing.getProperties().setSpeed(null);
        inputProcessedBsm8 = processedBsmSpeedMissing.toString();
    }

    @Test
    public void testSerialization() throws JsonMappingException, JsonProcessingException {
        ProcessedBsm<Point> processedBsm = objectMapper.readValue(inputProcessedBsm1,
                new TypeReference<ProcessedBsm<Point>>() {
                });
        String json = processedBsm.toString();
        assertEquals(inputProcessedBsm1, json);
    }

    @Test
    public void testJsonSerdes() throws IOException {
        Serde<ProcessedBsm<Point>> serdes = JsonSerdes.ProcessedBsm();
        ProcessedBsm<Point> deserialized = serdes.deserializer().deserialize(null, inputProcessedBsm1.getBytes());
        byte[] serialized = serdes.serializer().serialize(null, deserialized);
        String serializedString = new String(serialized);
        assertThat(serializedString, jsonEquals(inputProcessedBsm1));
    }

    @Test
    public void testTopology() {
        props = new DeduplicatorProperties();
        props.setAlwaysIncludeAtSpeed(1);
        props.setEnableProcessedBsmDeduplication(true);
        props.setProcessedBsmMaximumPositionDelta(1);
        props.setProcessedBsmMaximumTimeDelta(10000);
        props.setKafkaTopicProcessedBsm(inputTopic);
        props.setKafkaTopicDeduplicatedProcessedBsm(outputTopic);

        ProcessedBsmDeduplicatorTopology processedBsmDeduplicatorTopology = new ProcessedBsmDeduplicatorTopology(props);
        Topology topology = processedBsmDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<String, String> inputProcessedBsmData = driver.createInputTopic(
                    inputTopic,
                    Serdes.String().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<String, ProcessedBsm<Point>> outputProcessedBsmData = driver.createOutputTopic(
                    outputTopic,
                    Serdes.String().deserializer(),
                    JsonSerdes.ProcessedBsm().deserializer());

            inputProcessedBsmData.pipeInput(key, inputProcessedBsm1);
            inputProcessedBsmData.pipeInput(key, inputProcessedBsm2);
            inputProcessedBsmData.pipeInput(key, inputProcessedBsm3);
            inputProcessedBsmData.pipeInput(key, inputProcessedBsm4);
            inputProcessedBsmData.pipeInput(key, inputProcessedBsm5);
            inputProcessedBsmData.pipeInput(key, inputProcessedBsm6);
            inputProcessedBsmData.pipeInput(key, inputProcessedBsm7);
            inputProcessedBsmData.pipeInput(key, inputProcessedBsm8);

            List<KeyValue<String, ProcessedBsm<Point>>> processedBsmDeduplicationResults = outputProcessedBsmData
                    .readKeyValuesToList();

            // validate that only 7 messages make it through
            assertEquals(7, processedBsmDeduplicationResults.size());

            ProcessedBsm<Point> processedBsm1 = objectMapper.readValue(inputProcessedBsm1,
                    new TypeReference<ProcessedBsm<Point>>() {
                    });
            ProcessedBsm<Point> processedBsm3 = objectMapper.readValue(inputProcessedBsm3,
                    new TypeReference<ProcessedBsm<Point>>() {
                    });
            ProcessedBsm<Point> processedBsm4 = objectMapper.readValue(inputProcessedBsm4,
                    new TypeReference<ProcessedBsm<Point>>() {
                    });
            ProcessedBsm<Point> processedBsm5 = objectMapper.readValue(inputProcessedBsm5,
                    new TypeReference<ProcessedBsm<Point>>() {
                    });
            ProcessedBsm<Point> processedBsm6 = objectMapper.readValue(inputProcessedBsm6,
                    new TypeReference<ProcessedBsm<Point>>() {
                    });
            ProcessedBsm<Point> processedBsm7 = objectMapper.readValue(inputProcessedBsm7,
                    new TypeReference<ProcessedBsm<Point>>() {
                    });
            ProcessedBsm<Point> processedBsm8 = objectMapper.readValue(inputProcessedBsm8,
                    new TypeReference<ProcessedBsm<Point>>() {
                    });

            assertThat(processedBsmDeduplicationResults.get(0).value.toString(), jsonEquals(processedBsm1.toString()));
            assertThat(processedBsmDeduplicationResults.get(1).value.toString(), jsonEquals(processedBsm3.toString()));
            assertThat(processedBsmDeduplicationResults.get(2).value.toString(), jsonEquals(processedBsm4.toString()));
            assertThat(processedBsmDeduplicationResults.get(3).value.toString(), jsonEquals(processedBsm5.toString()));
            assertThat(processedBsmDeduplicationResults.get(4).value.toString(), jsonEquals(processedBsm6.toString()));
            assertThat(processedBsmDeduplicationResults.get(5).value.toString(), jsonEquals(processedBsm7.toString()));
            assertThat(processedBsmDeduplicationResults.get(6).value.toString(), jsonEquals(processedBsm8.toString()));

        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}