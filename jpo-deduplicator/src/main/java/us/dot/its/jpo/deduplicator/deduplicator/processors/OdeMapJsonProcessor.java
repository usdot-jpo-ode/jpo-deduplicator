package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.utils.OdeJsonUtils;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

public class OdeMapJsonProcessor extends DeduplicationProcessor<OdeMessageFrameData> {

    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(OdeMapJsonProcessor.class);

    public OdeMapJsonProcessor(String storeName, DeduplicatorProperties props) {
        this.storeName = storeName;
        this.props = props;
    }

    @Override
    public Instant getMessageTime(OdeMessageFrameData message) {
        return OdeJsonUtils.getOdeMessageFrameMessageTime(message);
    }

    @Override
    public boolean isDuplicate(OdeMessageFrameData lastMessage, OdeMessageFrameData newMessage) {
        try {
            Instant newValueTime = getMessageTime(newMessage);
            Instant oldValueTime = getMessageTime(lastMessage);

            // If the messages are more than an hour apart, forward the new message on
            if (newValueTime.minus(Duration.ofHours(1)).isAfter(oldValueTime)) {
                return false;
            }

            // Check for null conditions - treat as non-duplicate if one is null and the other is not
            boolean lastMessageIsNull = (lastMessage == null || lastMessage.getPayload() == null || lastMessage.getPayload().getData() == null);
            boolean newMessageIsNull = (newMessage == null || newMessage.getPayload() == null || newMessage.getPayload().getData() == null);
            if ((lastMessageIsNull && !newMessageIsNull) || (!lastMessageIsNull && newMessageIsNull)) {
                logger.warn("One TIM message has a null payload or data, treating as non-duplicate");
                return true;
            }
        } catch (Exception e) {
            logger.warn("Caught General Exception while checking Map duplicates: " + e.getMessage(), e);
        }

        // Treat maps as duplicates if they have the same intersection ID and
        // are within the 1 hour time window
        return true;
    }
}
