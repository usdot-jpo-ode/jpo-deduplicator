package us.dot.its.jpo.deduplicator.deduplicator;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

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

import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessageMessageFrame;
import us.dot.its.jpo.asn.j2735.r2024.Common.BSMcoreData;
import us.dot.its.jpo.asn.j2735.r2024.Common.Latitude;
import us.dot.its.jpo.asn.j2735.r2024.Common.Longitude;
import us.dot.its.jpo.asn.j2735.r2024.Common.Speed;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.BsmDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;

public class BsmDeduplicatorTopologyTest {

    String inputTopic = "topic.OdeBsmJson";
    String outputTopic = "topic.DeduplicatedOdeBsmJson";
    ObjectMapper objectMapper;

    String inputBsm1;
    // Same as BSM 1 - No Message should be generated
    String inputBsm2;

    // Increase Time from Bsm 1, Should be forwarded
    String inputBsm3;

    // Vehicle Speed not 0, Should be Forwarded
    String inputBsm4;

    // Vehicle Position has changed, Should be Forwarded
    String inputBsm5;

    // Vehicle Position was removed. Should be Forwarded
    String inputBsm6;

    // Vehicle Position was added back in. Should be Forwarded
    String inputBsm7;

    // Vehicle is missing its speed section, Should be Forwarded
    String inputBsm8;

    // Vehicle is missing its set to unavailable
    String inputBsm9;

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        objectMapper = new ObjectMapper();

        // Load test files from resources

        // Reference MAP
        String bsmReference = new String(
                Files.readAllBytes(Paths
                        .get("src/test/resources/json/ode_bsm/sample.ode-bsm-reference.json")));
        OdeMessageFrameData bsmReferenceData = objectMapper.readValue(bsmReference, OdeMessageFrameData.class);

        // Reference BSM, Should be forwarded
        inputBsm1 = bsmReferenceData.toJson();

        // Duplicate of Number 1, should be dropped
        inputBsm2 = bsmReferenceData.toJson();

        // Increase Time from Bsm 1, Should be forwarded
        OdeMessageFrameData bsmIncreaseTime = objectMapper.readValue(bsmReferenceData.toJson(),
                OdeMessageFrameData.class);
        String originalTime = bsmIncreaseTime.getMetadata().getOdeReceivedAt();
        Instant instant = Instant.parse(originalTime);
        Instant newInstant = instant.plusMillis(15000);
        bsmIncreaseTime.getMetadata().setOdeReceivedAt(newInstant.toString());
        inputBsm3 = bsmIncreaseTime.toJson();

        // Vehicle Speed not 0, Should be Forwarded
        OdeMessageFrameData bsmSpeedNotZero = objectMapper.readValue(bsmReferenceData.toJson(),
                OdeMessageFrameData.class);
        BasicSafetyMessageMessageFrame bsmMfSpeedNotZero = (BasicSafetyMessageMessageFrame) bsmSpeedNotZero
                .getPayload()
                .getData();
        bsmMfSpeedNotZero.getValue().getCoreData().setSpeed(new Speed(105));
        inputBsm4 = bsmSpeedNotZero.toJson();

        // Vehicle Position has changed, Should be Forwarded
        OdeMessageFrameData bsmPositionChanged = objectMapper.readValue(bsmReferenceData.toJson(),
                OdeMessageFrameData.class);
        BasicSafetyMessageMessageFrame bsmMfPositionChanged = (BasicSafetyMessageMessageFrame) bsmPositionChanged
                .getPayload().getData();
        BSMcoreData coreData = bsmMfPositionChanged.getValue().getCoreData();
        coreData.setLat(new Latitude(400000000));
        coreData.setLong_(new Longitude(-1050000000));
        inputBsm5 = bsmPositionChanged.toJson();

        // Vehicle Position was removed. Should be Forwarded
        OdeMessageFrameData bsmPositionRemoved = objectMapper.readValue(bsmReferenceData.toJson(),
                OdeMessageFrameData.class);
        BasicSafetyMessageMessageFrame bsmMfPositionRemoved = (BasicSafetyMessageMessageFrame) bsmPositionRemoved
                .getPayload().getData();
        bsmMfPositionRemoved.getValue().getCoreData().setLat(null);
        bsmMfPositionRemoved.getValue().getCoreData().setLong_(null);
        inputBsm6 = bsmPositionRemoved.toJson();

        // Vehicle Position was added back in. Should be Forwarded
        OdeMessageFrameData bsmPositionAddedBack = objectMapper.readValue(bsmReferenceData.toJson(),
                OdeMessageFrameData.class);
        inputBsm7 = bsmPositionAddedBack.toJson();

        // Vehicle is missing its speed section, Should be Forwarded
        OdeMessageFrameData bsmSpeedMissing = objectMapper.readValue(bsmReferenceData.toJson(),
                OdeMessageFrameData.class);
        BasicSafetyMessageMessageFrame bsmMfSpeedMissing = (BasicSafetyMessageMessageFrame) bsmSpeedMissing
                .getPayload().getData();
        bsmMfSpeedMissing.getValue().getCoreData().setSpeed(null);
        inputBsm8 = bsmSpeedMissing.toJson();

        // Vehicle is missing its set to unavailable, should be forwarded
        OdeMessageFrameData bsmSetToUnavailable = objectMapper.readValue(bsmReferenceData.toJson(),
                OdeMessageFrameData.class);
        BasicSafetyMessageMessageFrame bsmMfSetToUnavailable = (BasicSafetyMessageMessageFrame) bsmSetToUnavailable
                .getPayload().getData();
        bsmMfSetToUnavailable.getValue().getCoreData().setSpeed(new Speed(8191));
        inputBsm9 = bsmSetToUnavailable.toJson();

    }

    @Test
    public void testTopology() {
        props = new DeduplicatorProperties();
        props.setAlwaysIncludeAtSpeed(1);
        props.setenableOdeBsmDeduplication(true);
        props.setMaximumPositionDelta(1);
        props.setMaximumTimeDelta(10000);
        props.setkafkaTopicOdeBsmJson(inputTopic);
        props.setkafkaTopicDeduplicatedOdeBsmJson(outputTopic);

        BsmDeduplicatorTopology bsmDeduplicatorTopology = new BsmDeduplicatorTopology(props);
        Topology topology = bsmDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<Void, String> inputOdeBsmData = driver.createInputTopic(
                    inputTopic,
                    Serdes.Void().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<String, OdeMessageFrameData> outputOdeBsmData = driver.createOutputTopic(
                    outputTopic,
                    Serdes.String().deserializer(),
                    JsonSerdes.OdeMessageFrame().deserializer());

            inputOdeBsmData.pipeInput(null, inputBsm1);
            inputOdeBsmData.pipeInput(null, inputBsm2);
            inputOdeBsmData.pipeInput(null, inputBsm3);
            inputOdeBsmData.pipeInput(null, inputBsm4);
            inputOdeBsmData.pipeInput(null, inputBsm5);
            inputOdeBsmData.pipeInput(null, inputBsm6);
            inputOdeBsmData.pipeInput(null, inputBsm7);
            inputOdeBsmData.pipeInput(null, inputBsm8);
            inputOdeBsmData.pipeInput(null, inputBsm9);

            List<KeyValue<String, OdeMessageFrameData>> bsmDeduplicationResults = outputOdeBsmData
                    .readKeyValuesToList();

            // validate that only 8 messages make it through
            assertEquals(7, bsmDeduplicationResults.size());

            objectMapper = new ObjectMapper();
            OdeMessageFrameData bsm1 = objectMapper.readValue(inputBsm1, OdeMessageFrameData.class);
            OdeMessageFrameData bsm3 = objectMapper.readValue(inputBsm3, OdeMessageFrameData.class);
            OdeMessageFrameData bsm4 = objectMapper.readValue(inputBsm4, OdeMessageFrameData.class);
            OdeMessageFrameData bsm5 = objectMapper.readValue(inputBsm5, OdeMessageFrameData.class);
            OdeMessageFrameData bsm6 = objectMapper.readValue(inputBsm6, OdeMessageFrameData.class);
            OdeMessageFrameData bsm7 = objectMapper.readValue(inputBsm7, OdeMessageFrameData.class);
            OdeMessageFrameData bsm8 = objectMapper.readValue(inputBsm8, OdeMessageFrameData.class);

            assertThat(bsmDeduplicationResults.get(0).value.toJson(), jsonEquals(bsm1.toJson()));
            assertThat(bsmDeduplicationResults.get(1).value.toJson(), jsonEquals(bsm3.toJson()));
            assertThat(bsmDeduplicationResults.get(2).value.toJson(), jsonEquals(bsm4.toJson()));
            assertThat(bsmDeduplicationResults.get(3).value.toJson(), jsonEquals(bsm5.toJson()));
            assertThat(bsmDeduplicationResults.get(4).value.toJson(), jsonEquals(bsm6.toJson()));
            assertThat(bsmDeduplicationResults.get(5).value.toJson(), jsonEquals(bsm7.toJson()));
            assertThat(bsmDeduplicationResults.get(6).value.toJson(), jsonEquals(bsm8.toJson()));

        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}