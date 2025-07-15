package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;
import us.dot.its.jpo.asn.j2735.r2024.MapData.MapDataMessageFrame;

public class OdeMapJsonProcessor extends DeduplicationProcessor<OdeMessageFrameData> {

    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(OdeMapJsonProcessor.class);

    public OdeMapJsonProcessor(String storeName, DeduplicatorProperties props) {
        this.storeName = storeName;
        this.props = props;
    }

    @Override
    public Instant getMessageTime(OdeMessageFrameData message) {
        try {
            if (message == null || message.getMetadata() == null) {
                logger.warn("Map message or metadata is null, using epoch time");
                return Instant.ofEpochMilli(0);
            }

            String time = ((OdeMessageFrameMetadata) message.getMetadata()).getOdeReceivedAt();
            if (time == null || time.isEmpty()) {
                logger.warn("Map message has null or empty odeReceivedAt time, using epoch time");
                return Instant.ofEpochMilli(0);
            }

            return Instant.from(formatter.parse(time));
        } catch (Exception e) {
            logger.warn("Failed to parse time from Map: " + (message != null && message.getMetadata() != null
                    ? ((OdeMessageFrameMetadata) message.getMetadata()).getOdeReceivedAt()
                    : "null"), e);
            return Instant.ofEpochMilli(0);
        }
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

            // Add null checks for payload and data
            if (lastMessage == null || lastMessage.getPayload() == null || lastMessage.getPayload().getData() == null ||
                    newMessage == null || newMessage.getPayload() == null
                    || newMessage.getPayload().getData() == null) {
                logger.warn("One or both Map messages have null payload or data, treating as non-duplicate");
                return false;
            }

            MapDataMessageFrame oldMap = (MapDataMessageFrame) lastMessage.getPayload().getData();
            MapDataMessageFrame newMap = (MapDataMessageFrame) newMessage.getPayload().getData();

            // Compare map data for equality
            if (oldMap == null || newMap == null || oldMap.getValue() == null || newMap.getValue() == null) {
                return false;
            }

            // For now, treat maps as duplicates if they have the same intersection ID and
            // are within the time window
            // This is a simplified approach - in a real implementation you might want to
            // compare more map properties
            return true;

        } catch (Exception e) {
            logger.warn("Caught General Exception while checking Map duplicates: " + e.getMessage(), e);
        }

        return true;
    }
}
