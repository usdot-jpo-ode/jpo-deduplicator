// package us.dot.its.jpo.deduplicator.deduplicator.topologies;

// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.KafkaStreams;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.Topology;
// import org.apache.kafka.streams.KafkaStreams.StateListener;
// import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

// import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;
// import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
// import org.apache.kafka.streams.kstream.*;
// import org.apache.kafka.streams.state.Stores;
// import org.geotools.referencing.GeodeticCalculator;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.fasterxml.jackson.databind.ObjectMapper;

// import java.time.format.DateTimeFormatter;
// import java.util.Properties;

// import
// us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers.ProcessedBsmJsonProcessorSupplier;
// import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
// import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
// import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;

// public class ProcessedBsmDeduplicatorTopology {

// private static final Logger logger =
// LoggerFactory.getLogger(ProcessedBsmDeduplicatorTopology.class);

// Topology topology;
// KafkaStreams streams;
// DeduplicatorProperties props;
// ObjectMapper objectMapper;
// DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
// GeodeticCalculator calculator;
// Properties streamsProperties;

// public ProcessedBsmDeduplicatorTopology(DeduplicatorProperties props,
// Properties streamsProperties) {
// this.props = props;
// this.streamsProperties = streamsProperties;
// this.objectMapper = DateJsonMapper.getInstance();
// }

// public void start() {
// if (streams != null && streams.state().isRunningOrRebalancing()) {
// throw new IllegalStateException("Start called while streams is already
// running.");
// }
// Topology topology = buildTopology();
// streams = new KafkaStreams(topology, streamsProperties);
// if (exceptionHandler != null)
// streams.setUncaughtExceptionHandler(exceptionHandler);
// if (stateListener != null)
// streams.setStateListener(stateListener);
// logger.info("Starting Processed Bsm Deduplicator Topology");
// streams.start();
// }

// public Topology buildTopology() {
// StreamsBuilder builder = new StreamsBuilder();

// KStream<String, ProcessedBsm<Point>> inputStream =
// builder.stream(this.props.getKafkaTopicProcessedBsm(),
// Consumed.with(Serdes.String(), JsonSerdes.ProcessedBsm()));

// builder.addStateStore(
// Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(props.getKafkaStateStoreProcessedBsmName()),
// Serdes.String(), JsonSerdes.ProcessedBsm()));

// KStream<String, ProcessedBsm<Point>> deduplicatedStream =
// inputStream.process(
// new
// ProcessedBsmJsonProcessorSupplier(props.getKafkaStateStoreProcessedBsmName(),
// props),
// props.getKafkaStateStoreProcessedBsmName());

// deduplicatedStream.to(this.props.getKafkaTopicDeduplicatedProcessedBsm(),
// Produced.with(Serdes.String(), JsonSerdes.ProcessedBsm()));

// return builder.build();

// }

// public void stop() {
// logger.info("Stopping Processed Bsm deduplicator Topology.");
// if (streams != null) {
// streams.close();
// streams.cleanUp();
// streams = null;
// }
// logger.info("Stopped Processed Bsm deduplicator Topology.");
// }

// StateListener stateListener;

// public void registerStateListener(StateListener stateListener) {
// this.stateListener = stateListener;
// }

// StreamsUncaughtExceptionHandler exceptionHandler;

// public void registerUncaughtExceptionHandler(StreamsUncaughtExceptionHandler
// exceptionHandler) {
// this.exceptionHandler = exceptionHandler;
// }
// }
