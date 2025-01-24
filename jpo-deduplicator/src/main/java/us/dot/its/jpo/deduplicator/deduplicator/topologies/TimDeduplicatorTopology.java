package us.dot.its.jpo.deduplicator.deduplicator.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.model.OdeTimMetadata;
import us.dot.its.jpo.ode.model.OdeTimPayload;
import us.dot.its.jpo.ode.plugin.j2735.OdeTravelerInformationMessage;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerInformation;

import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers.OdeTimJsonProcessorSupplier;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;


public class TimDeduplicatorTopology {

    private static final Logger logger = LoggerFactory.getLogger(TimDeduplicatorTopology.class);

    Topology topology;
    KafkaStreams streams;
    Properties streamsProperties;
    ObjectMapper objectMapper;
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    DeduplicatorProperties props;

    public TimDeduplicatorTopology(DeduplicatorProperties props, Properties streamsProperties) {
        this.props = props;
        this.streamsProperties = streamsProperties;
        this.objectMapper = DateJsonMapper.getInstance();
    }

    public void start() {
        if (streams != null && streams.state().isRunningOrRebalancing()) {
            throw new IllegalStateException("Start called while streams is already running.");
        }
        Topology topology = buildTopology();
        streams = new KafkaStreams(topology, streamsProperties);
        if (exceptionHandler != null)
            streams.setUncaughtExceptionHandler(exceptionHandler);
        if (stateListener != null)
            streams.setStateListener(stateListener);
        logger.info("Starting Tim Deduplicator Topology");
        streams.start();
    }

    public JsonNode genJsonNode() {
        return objectMapper.createObjectNode();
    }

    

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, OdeTimData> inputStream = builder.stream(props.getKafkaTopicOdeTimJson(),
                Consumed.with(Serdes.String(), JsonSerdes.OdeTim()));

        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(props.getKafkaStateStoreOdeTimJsonName()),
                Serdes.String(), JsonSerdes.OdeTim()));

        KStream<String, OdeTimData> timRekeyedStream = inputStream.selectKey((key, value) -> {
            try {

                TravelerInformation travellerInformation = (TravelerInformation)value.getPayload().getData();
            

                String rsuIP = ((OdeTimMetadata)value.getMetadata()).getOriginIp();
                // String packetId = ((OdeTimPayload)value.getPayload()).getData();//   .get("packetID").asText();
                String packetId = travellerInformation.getPacketID().toString();
                int msgCnt = travellerInformation.getMsgCnt().intValue();

                String newKey = rsuIP + "_" + packetId + "_" + msgCnt;
                return newKey;
            } catch (Exception e) {
                System.out.println(e);
                return "";
            }
        }).repartition(Repartitioned.with(Serdes.String(), JsonSerdes.OdeTim()));

        KStream<String, OdeTimData> deduplicatedStream = timRekeyedStream.process(new OdeTimJsonProcessorSupplier(props), props.getKafkaStateStoreOdeTimJsonName());

        deduplicatedStream.to(props.getKafkaTopicDeduplicatedOdeTimJson(), Produced.with(Serdes.String(), JsonSerdes.OdeTim()));

        return builder.build();

    }

    public void stop() {
        logger.info("Stopping Tim deduplicator Socket Broadcast Topology.");
        if (streams != null) {
            streams.close();
            streams.cleanUp();
            streams = null;
        }
        logger.info("Stopped Tim deduplicator Socket Broadcast Topology.");
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
