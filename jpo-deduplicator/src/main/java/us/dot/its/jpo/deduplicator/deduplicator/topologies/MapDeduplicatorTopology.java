package us.dot.its.jpo.deduplicator.deduplicator.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import us.dot.its.jpo.asn.j2735.r2024.MapData.MapDataMessageFrame;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.format.DateTimeFormatter;

import us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers.OdeMapJsonProcessorSupplier;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
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
                    logger.warn("Received Map message with null payload or data, discarding message");
                    return "unknown";
                }

                MapDataMessageFrame mf = (MapDataMessageFrame) value.getPayload().getData();
                if (mf == null || mf.getValue() == null || mf.getValue().getIntersections() == null ||
                        mf.getValue().getIntersections().isEmpty()) {
                    logger.warn("Received Map message with null message frame data, discarding message");
                    return "unknown";
                }

                RsuIntersectionKey newKey = new RsuIntersectionKey();
                newKey.setRsuId(((OdeMessageFrameMetadata) value.getMetadata()).getOriginIp());
                newKey.setIntersectionReferenceID(mf.getValue().getIntersections().get(0).getId());
                return newKey.toString();
            } catch (Exception e) {
                logger.error("Error extracting key from Map message: " + e.getMessage() + ", discarding message", e);
                return "unknown";
            }
        })
        .filter((key, value) -> {
            if ("unknown".equals(key)) {
                logger.debug("Discarding Map message with unknown key");
                return false;
            }
            return true;
        })
        .repartition(Repartitioned.with(Serdes.String(), JsonSerdes.OdeMessageFrame()));

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
