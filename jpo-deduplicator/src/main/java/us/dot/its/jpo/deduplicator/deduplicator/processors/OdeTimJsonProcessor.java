package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.model.OdeTimMetadata;

public class OdeTimJsonProcessor extends DeduplicationProcessor<OdeTimData>{

    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    private static final Logger logger = LoggerFactory.getLogger(OdeTimJsonProcessor.class);

    DeduplicatorProperties props;
    public OdeTimJsonProcessor(DeduplicatorProperties props){
        this.props = props;
        this.storeName = props.getKafkaStateStoreOdeTimJsonName();
    }


    @Override
    public Instant getMessageTime(OdeTimData message) {
        try {
            // String time = message.get("metadata").get("odeReceivedAt").asText();
            String time = ((OdeTimMetadata)message.getMetadata()).getOdeReceivedAt();
            return Instant.from(formatter.parse(time));
        } catch (Exception e) {
            return Instant.ofEpochMilli(0);
        }
    }

    @Override
    public boolean isDuplicate(OdeTimData lastMessage, OdeTimData newMessage) {
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
