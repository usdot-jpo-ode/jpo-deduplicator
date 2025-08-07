package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;

public class ProcessedMapWktProcessor extends DeduplicationProcessor<ProcessedMap<String>>{

    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(ProcessedMapWktProcessor.class);

    public ProcessedMapWktProcessor(DeduplicatorProperties props){
        this.props = props;
        this.storeName = props.getKafkaStateStoreProcessedMapWKTName();
    }


    @Override
    public Instant getMessageTime(ProcessedMap<String> message) {
        try {
            return message.getProperties().getOdeReceivedAt().toInstant();
        } catch (Exception e) {
            return Instant.ofEpochMilli(0);
        }
    }

    @Override
    public boolean isDuplicate(ProcessedMap<String> lastMessage, ProcessedMap<String> newMessage) {
        try{
            Instant newValueTime = getMessageTime(newMessage);
            Instant oldValueTime = getMessageTime(lastMessage);
            
            if(newValueTime.minus(Duration.ofHours(1)).isAfter(oldValueTime)){
                return false;
            }else{
                ZonedDateTime newValueTimestamp = newMessage.getProperties().getTimeStamp();
                ZonedDateTime newValueOdeReceivedAt = newMessage.getProperties().getOdeReceivedAt();

                newMessage.getProperties().setTimeStamp(lastMessage.getProperties().getTimeStamp());
                newMessage.getProperties().setOdeReceivedAt(lastMessage.getProperties().getOdeReceivedAt());

                int oldHash = lastMessage.getProperties().hashCode();
                int newhash = newMessage.getProperties().hashCode();

                if(oldHash != newhash){
                    newMessage.getProperties().setTimeStamp(newValueTimestamp);
                    newMessage.getProperties().setOdeReceivedAt(newValueOdeReceivedAt);
                    return false;
                }
            }
        } catch(Exception e){
            logger.warn("Caught General Exception" + e);
        }

        return true;
    }
}
