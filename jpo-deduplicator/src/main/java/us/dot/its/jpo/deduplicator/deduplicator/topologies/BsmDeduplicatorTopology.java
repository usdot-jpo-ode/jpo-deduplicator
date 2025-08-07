package us.dot.its.jpo.deduplicator.deduplicator.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessageMessageFrame;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.format.DateTimeFormatter;

import us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers.OdeBsmJsonProcessorSupplier;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;

public class BsmDeduplicatorTopology {

    private static final Logger logger = LoggerFactory.getLogger(BsmDeduplicatorTopology.class);

    Topology topology;
    KafkaStreams streams;
    DeduplicatorProperties props;
    ObjectMapper objectMapper;
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    public BsmDeduplicatorTopology(DeduplicatorProperties props) {
        this.props = props;
        this.objectMapper = DateJsonMapper.getInstance();
    }

    public void start() {
        if (streams != null && streams.state().isRunningOrRebalancing()) {
            throw new IllegalStateException("Start called while streams is already running.");
        }
        Topology topology = buildTopology();
        streams = new KafkaStreams(topology, props.createStreamProperties("BsmDeduplicator"));
        if (exceptionHandler != null)
            streams.setUncaughtExceptionHandler(exceptionHandler);
        if (stateListener != null)
            streams.setStateListener(stateListener);
        logger.info("Starting Bsm Deduplicator Topology");
        streams.start();
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, OdeMessageFrameData> inputStream = builder.stream(this.props.getKafkaTopicOdeBsmJson(),
                Consumed.with(Serdes.Void(), JsonSerdes.OdeMessageFrame()));

        builder.addStateStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(props.getKafkaStateStoreOdeBsmJsonName()),
                        Serdes.String(), JsonSerdes.OdeMessageFrame()));

        KStream<String, OdeMessageFrameData> bsmRekeyedStream = inputStream.selectKey((key, value) -> {
            try {
                if (value == null || value.getPayload() == null || value.getPayload().getData() == null) {
                    logger.warn("Received BSM message with null payload or data, using default key");
                    return "unknown";
                }

                BasicSafetyMessageMessageFrame mf = (BasicSafetyMessageMessageFrame) value.getPayload().getData();
                if (mf == null || mf.getValue() == null || mf.getValue().getCoreData() == null ||
                        mf.getValue().getCoreData().getId() == null) {
                    logger.warn("Received BSM message with null message frame data, using default key");
                    return "unknown";
                }

                return mf.getValue().getCoreData().getId().getValue();
            } catch (Exception e) {
                logger.error("Error extracting key from BSM message: " + e.getMessage(), e);
                return "unknown";
            }
        }).repartition(Repartitioned.with(Serdes.String(), JsonSerdes.OdeMessageFrame()));

        KStream<String, OdeMessageFrameData> deduplicatedStream = bsmRekeyedStream.process(
                new OdeBsmJsonProcessorSupplier(props.getKafkaStateStoreOdeBsmJsonName(), props),
                props.getKafkaStateStoreOdeBsmJsonName());

        deduplicatedStream.to(this.props.getKafkaTopicDeduplicatedOdeBsmJson(),
                Produced.with(Serdes.String(), JsonSerdes.OdeMessageFrame()));

        return builder.build();

    }

    public void stop() {
        logger.info("Stopping Bsm deduplicator Socket Broadcast Topology.");
        if (streams != null) {
            streams.close();
            streams.cleanUp();
            streams = null;
        }
        logger.info("Stopped Bsm deduplicator Socket Broadcast Topology.");
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
