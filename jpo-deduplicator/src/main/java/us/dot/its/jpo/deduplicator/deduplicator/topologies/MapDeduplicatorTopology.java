package us.dot.its.jpo.deduplicator.deduplicator.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers.OdeMapJsonProcessorSupplier;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;

public class MapDeduplicatorTopology {

    private static final Logger logger = LoggerFactory.getLogger(MapDeduplicatorTopology.class);

    Topology topology;
    KafkaStreams streams;
    DeduplicatorProperties props;
    ObjectMapper objectMapper;
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    public MapDeduplicatorTopology(DeduplicatorProperties props) {
        this.props = props;
        this.objectMapper = DateJsonMapper.getInstance();
    }

    public void start() {
        if (streams != null && streams.state().isRunningOrRebalancing()) {
            throw new IllegalStateException("Start called while streams is already running.");
        }
        Topology topology = buildTopology();
        streams = new KafkaStreams(topology, props.createStreamProperties("MapDeduplicator"));
        if (exceptionHandler != null)
            streams.setUncaughtExceptionHandler(exceptionHandler);
        if (stateListener != null)
            streams.setStateListener(stateListener);
        logger.info("Starting Map Deduplicator Topology");
        streams.start();
    }

    // public Instant getInstantFromMap(OdeMessageFrameData map) {
    // try {
    // if (map == null || map.getMetadata() == null) {
    // logger.warn("Map message or metadata is null, using epoch time");
    // return Instant.ofEpochMilli(0);
    // }

    // String time = ((OdeMessageFrameMetadata)
    // map.getMetadata()).getOdeReceivedAt();
    // if (time == null || time.isEmpty()) {
    // logger.warn("Map message has null or empty odeReceivedAt time, using epoch
    // time");
    // return Instant.ofEpochMilli(0);
    // }

    // return Instant.from(formatter.parse(time));
    // } catch (Exception e) {
    // logger.warn("Failed to parse time from Map: " + (map != null &&
    // map.getMetadata() != null
    // ? ((OdeMessageFrameMetadata) map.getMetadata()).getOdeReceivedAt()
    // : "null"), e);
    // return Instant.ofEpochMilli(0);
    // }
    // }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, OdeMessageFrameData> inputStream = builder.stream(props.getKafkaTopicOdeMapJson(),
                Consumed.with(Serdes.Void(), JsonSerdes.OdeMessageFrame()));

        builder.addStateStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(props.getKafkaStateStoreOdeMapJsonName()),
                        Serdes.String(), JsonSerdes.OdeMessageFrame()));

        KStream<String, OdeMessageFrameData> mapRekeyedStream = inputStream.selectKey((key, value) -> {
            try {
                if (value == null || value.getPayload() == null || value.getPayload().getData() == null) {
                    logger.warn("Received Map message with null payload or data, using default key");
                    return "unknown";
                }

                // For now, use a simple key based on RSU IP and timestamp
                // This can be enhanced later with proper intersection ID extraction
                String rsuIp = ((OdeMessageFrameMetadata) value.getMetadata()).getOriginIp();
                String timestamp = ((OdeMessageFrameMetadata) value.getMetadata()).getOdeReceivedAt();
                return rsuIp + "_" + timestamp;
            } catch (Exception e) {
                logger.error("Error extracting key from Map message: " + e.getMessage(), e);
                return "unknown";
            }
        }).repartition(Repartitioned.with(Serdes.String(), JsonSerdes.OdeMessageFrame()));

        KStream<String, OdeMessageFrameData> deduplicatedStream = mapRekeyedStream.process(
                new OdeMapJsonProcessorSupplier(props.getKafkaStateStoreOdeMapJsonName(), props),
                props.getKafkaStateStoreOdeMapJsonName());

        deduplicatedStream.to(props.getKafkaTopicDeduplicatedOdeMapJson(),
                Produced.with(Serdes.String(), JsonSerdes.OdeMessageFrame()));

        return builder.build();

    }

    public void stop() {
        logger.info("Stopping Map Deduplicator Socket Broadcast Topology.");
        if (streams != null) {
            streams.close();
            streams.cleanUp();
            streams = null;
        }
        logger.info("Stopped Map Deduplicator Socket Broadcast Topology.");
    }

    StateListener stateListener;

    public void registerStateListener(StateListener stateListener) {
        this.stateListener = stateListener;
    }

    StreamsUncaughtExceptionHandler exceptionHandler;

    public void registerUncaughtExceptionHandler(StreamsUncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }
}
