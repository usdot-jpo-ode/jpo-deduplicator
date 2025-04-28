package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;

public class OdeRawEncodedTimJsonProcessor extends DeduplicationProcessor<JsonNode>{

    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(OdeRawEncodedTimJsonProcessor.class);

    public OdeRawEncodedTimJsonProcessor(DeduplicatorProperties props){
        this.props = props;
        this.storeName = props.getKafkaStateStoreOdeRawEncodedTimJsonName();
    }


    @Override
    public Instant getMessageTime(JsonNode message) {
        try {
            String time = message.get("metadata").get("odeReceivedAt").asText();
            return Instant.from(formatter.parse(time));
        } catch (Exception e) {
            return Instant.ofEpochMilli(0);
        }
    }

    @Override
    public boolean isDuplicate(JsonNode lastMessage, JsonNode newMessage) {
        try{
            Instant oldValueTime = getMessageTime(lastMessage);
            Instant newValueTime = getMessageTime(newMessage);

            if(newValueTime.minus(Duration.ofHours(1)).isAfter(oldValueTime)){
                return false;
            }

        } catch(Exception e){
            logger.warn("Caught General Exception" + e);
        }
        return true;
    }
}
