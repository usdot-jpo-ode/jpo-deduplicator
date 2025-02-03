package us.dot.its.jpo.deduplicator.deduplicator;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;

import lombok.Getter;
import us.dot.its.jpo.conflictmonitor.monitor.MonitorServiceController;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.StreamsTopology;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.BsmDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.MapDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.TimDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.OdeRawEncodedTimDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedMapDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedMapWktDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedSpatDeduplicatorTopology;

@Controller
@DependsOn("createKafkaTopics")
@Profile("!test && !testConfig")
public class DeduplicatorServiceController {

    private static final Logger logger = LoggerFactory.getLogger(MonitorServiceController.class);

    // Temporary for KafkaStreams that don't implement the Algorithm interface
    @Getter
    final ConcurrentHashMap<String, KafkaStreams> streamsMap = new ConcurrentHashMap<String, KafkaStreams>();

    @Getter
    final ConcurrentHashMap<String, StreamsTopology> algoMap = new ConcurrentHashMap<String, StreamsTopology>();

   
    
    @Autowired
    public DeduplicatorServiceController(final DeduplicatorProperties props, 
            final KafkaTemplate<String, String> kafkaTemplate) {
       

        try {

            if(props.isEnableProcessedMapDeduplication()){
                logger.info("Starting Processed Map Deduplicator");
                ProcessedMapDeduplicatorTopology processedMapDeduplicatorTopology = new ProcessedMapDeduplicatorTopology(
                    props,
                    props.createStreamProperties("ProcessedMapDeduplicator")
                );
                processedMapDeduplicatorTopology.start();
            }
            
            if(props.isEnableProcessedMapWktDeduplication()){
                logger.info("Starting Processed Map WKT Deduplicator");
                ProcessedMapWktDeduplicatorTopology processedMapWktDeduplicatorTopology = new ProcessedMapWktDeduplicatorTopology(
                    props,
                    props.createStreamProperties("ProcessedMapWKTdeduplicator")
                );
                processedMapWktDeduplicatorTopology.start();
            }
            
            if(props.isEnableProcessedMapDeduplication()){
                logger.info("Starting Map Deduplicator");
                MapDeduplicatorTopology mapDeduplicatorTopology = new MapDeduplicatorTopology(
                    props,
                    props.createStreamProperties("MapDeduplicator")
                );
                mapDeduplicatorTopology.start();
            }
            
            if(props.isEnableOdeTimDeduplication()){
                logger.info("Starting Tim Deduplicator");
                TimDeduplicatorTopology timDeduplicatorTopology = new TimDeduplicatorTopology(
                    props,
                    props.createStreamProperties("TimDeduplicator")
                );
                timDeduplicatorTopology.start();
            }

            if(props.isEnableOdeRawEncodedTimDeduplication()){
                logger.info("Starting Raw Encoded TIM Deduplicator");
                OdeRawEncodedTimDeduplicatorTopology odeRawEncodedTimDeduplicatorTopology = new OdeRawEncodedTimDeduplicatorTopology(
                    props,
                    props.createStreamProperties("OdeRawEncodedTimDeduplicator")
                );
                odeRawEncodedTimDeduplicatorTopology.start();
            }

            if(props.isEnableProcessedSpatDeduplication()){
                logger.info("Starting Processed Spat Deduplicator");
                ProcessedSpatDeduplicatorTopology processedSpatDeduplicatorTopology = new ProcessedSpatDeduplicatorTopology(
                    props,
                    props.createStreamProperties("ProcessedSpatDeduplicator")
                );
                processedSpatDeduplicatorTopology.start();
            }

            if(props.isEnableOdeBsmDeduplication()){
                logger.info("Starting BSM Deduplicator");
                BsmDeduplicatorTopology bsmDeduplicatorTopology = new BsmDeduplicatorTopology(props);
                bsmDeduplicatorTopology.start();
            }
            

        } catch (Exception e) {
            logger.error("Encountered issue with creating topologies", e);
        }
    }
}
