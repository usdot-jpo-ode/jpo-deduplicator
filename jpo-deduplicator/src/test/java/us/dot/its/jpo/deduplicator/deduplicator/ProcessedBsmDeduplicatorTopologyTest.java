// package us.dot.its.jpo.deduplicator.deduplicator;

// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.KeyValue;
// import org.apache.kafka.streams.TestInputTopic;
// import org.apache.kafka.streams.TestOutputTopic;
// import org.apache.kafka.streams.Topology;
// import org.apache.kafka.streams.TopologyTestDriver;
// import org.junit.Before;
// import org.junit.Test;
// import org.springframework.beans.factory.annotation.Autowired;

// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.JsonMappingException;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.core.type.TypeReference;

// import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;
// import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
// import
// us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedBsmDeduplicatorTopology;
// import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
// import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;
// import static org.junit.jupiter.api.Assertions.assertEquals;

// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Paths;
// import java.util.List;

// public class ProcessedBsmDeduplicatorTopologyTest {

// String inputTopic = "topic.ProcessedBsmJson";
// String outputTopic = "topic.DeduplicatedOdeBsmJson";
// ObjectMapper objectMapper;

// String inputProcessedBsmReference = "";

// // Same as BSM 1 - No Message should be generated
// String inputProcessedBsmDuplicate = "";

// // Increase Time from Bsm 1
// String inputProcessedBsmTimeDelta = "";

// // Vehicle Speed not 0
// String inputProcessedBsmWithSpeed = "";

// // Vehicle Position has changed
// String inputProcessedBsmChanged = "";

// // Vehicle Position was removed
// String inputProcessedBsmMissingPosition = "";

// // Vehicle Position was added back in
// String inputProcessedBsmWithPosition = "";

// // Vehicle Speed was added back in
// String inputProcessedBsmMissingSpeed = "";

// String key =
// "{\"rsuId\":\"172.19.0.1\",\"logId\":\"1234567890\",\"bsmId\":\"31325433\"}";

// @Autowired
// DeduplicatorProperties props;

// @Before
// public void setup() throws IOException {
// inputProcessedBsmReference = new
// String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_bsm/sample.processed_bsm-reference.json")));
// inputProcessedBsmDuplicate = inputProcessedBsmReference;
// inputProcessedBsmTimeDelta = new
// String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_bsm/sample.processed_bsm-reference-10-seconds-later.json")));
// inputProcessedBsmWithSpeed = new
// String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_bsm/sample.processed_bsm-reference-with-speed.json")));
// inputProcessedBsmChanged = new
// String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_bsm/sample.processed_bsm-different.json")));
// inputProcessedBsmMissingPosition = new
// String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_bsm/sample.processed_bsm-missing-position.json")));
// inputProcessedBsmWithPosition = new
// String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_bsm/sample.processed_bsm-with-position.json")));
// inputProcessedBsmMissingSpeed = new
// String(Files.readAllBytes(Paths.get("src/test/resources/json/processed_bsm/sample.processed_bsm-missing-speed.json")));

// }

// @Test
// public void testTopology() {
// props = new DeduplicatorProperties();
// props.setAlwaysIncludeAtSpeed(1);
// props.setEnableProcessedBsmDeduplication(true);
// props.setProcessedBsmMaximumPositionDelta(1);
// props.setProcessedBsmMaximumTimeDelta(10000);
// props.setKafkaTopicProcessedBsm(inputTopic);
// props.setKafkaTopicDeduplicatedProcessedBsm(outputTopic);

// ProcessedBsmDeduplicatorTopology processedBsmDeduplicatorTopology = new
// ProcessedBsmDeduplicatorTopology(props, null);
// Topology topology = processedBsmDeduplicatorTopology.buildTopology();

// try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

// TestInputTopic<String, String> inputProcessedBsmData =
// driver.createInputTopic(
// inputTopic,
// Serdes.String().serializer(),
// Serdes.String().serializer());

// TestOutputTopic<String, ProcessedBsm<Point>> outputProcessedBsmData =
// driver.createOutputTopic(
// outputTopic,
// Serdes.String().deserializer(),
// JsonSerdes.ProcessedBsm().deserializer());

// inputProcessedBsmData.pipeInput(key, inputProcessedBsmReference);
// inputProcessedBsmData.pipeInput(key, inputProcessedBsmDuplicate);
// inputProcessedBsmData.pipeInput(key, inputProcessedBsmTimeDelta);
// inputProcessedBsmData.pipeInput(key, inputProcessedBsmWithSpeed);
// inputProcessedBsmData.pipeInput(key, inputProcessedBsmChanged);
// inputProcessedBsmData.pipeInput(key, inputProcessedBsmMissingPosition);
// inputProcessedBsmData.pipeInput(key, inputProcessedBsmWithPosition);
// inputProcessedBsmData.pipeInput(key, inputProcessedBsmMissingSpeed);

// List<KeyValue<String, ProcessedBsm<Point>>> processedBsmDeduplicationResults
// = outputProcessedBsmData.readKeyValuesToList();

// // validate that only 7 messages make it through
// assertEquals(7, processedBsmDeduplicationResults.size());

// objectMapper = new ObjectMapper();
// ProcessedBsm<Point> outputProcessedBsmReference =
// objectMapper.readValue(inputProcessedBsmReference, new
// TypeReference<ProcessedBsm<Point>>() {});
// ProcessedBsm<Point> outputProcessedBsmTimeDelta =
// objectMapper.readValue(inputProcessedBsmTimeDelta, new
// TypeReference<ProcessedBsm<Point>>() {});
// ProcessedBsm<Point> outputProcessedBsmWithSpeed =
// objectMapper.readValue(inputProcessedBsmWithSpeed, new
// TypeReference<ProcessedBsm<Point>>() {});
// ProcessedBsm<Point> outputProcessedBsmChanged =
// objectMapper.readValue(inputProcessedBsmChanged, new
// TypeReference<ProcessedBsm<Point>>() {});
// ProcessedBsm<Point> outputProcessedBsmMissingPosition =
// objectMapper.readValue(inputProcessedBsmChanged, new
// TypeReference<ProcessedBsm<Point>>() {});
// ProcessedBsm<Point> outputProcessedBsmWithPosition =
// objectMapper.readValue(inputProcessedBsmWithPosition, new
// TypeReference<ProcessedBsm<Point>>() {});
// ProcessedBsm<Point> outputProcessedBsmMissingSpeed =
// objectMapper.readValue(inputProcessedBsmMissingSpeed, new
// TypeReference<ProcessedBsm<Point>>() {});

// assertEquals(outputProcessedBsmReference.getProperties().getOdeReceivedAt(),
// processedBsmDeduplicationResults.get(0).value.getProperties().getOdeReceivedAt());
// assertEquals(outputProcessedBsmTimeDelta.getProperties().getOdeReceivedAt(),
// processedBsmDeduplicationResults.get(1).value.getProperties().getOdeReceivedAt());
// assertEquals(outputProcessedBsmWithSpeed.getProperties().getOdeReceivedAt(),
// processedBsmDeduplicationResults.get(2).value.getProperties().getOdeReceivedAt());
// assertEquals(outputProcessedBsmChanged.getProperties().getOdeReceivedAt(),
// processedBsmDeduplicationResults.get(3).value.getProperties().getOdeReceivedAt());
// assertEquals(outputProcessedBsmMissingPosition.getProperties().getOdeReceivedAt(),
// processedBsmDeduplicationResults.get(4).value.getProperties().getOdeReceivedAt());
// assertEquals(outputProcessedBsmWithPosition.getProperties().getOdeReceivedAt(),
// processedBsmDeduplicationResults.get(5).value.getProperties().getOdeReceivedAt());
// assertEquals(outputProcessedBsmMissingSpeed.getProperties().getOdeReceivedAt(),
// processedBsmDeduplicationResults.get(6).value.getProperties().getOdeReceivedAt());

// } catch (JsonMappingException e) {
// e.printStackTrace();
// } catch (JsonProcessingException e) {
// e.printStackTrace();
// }
// }
// }