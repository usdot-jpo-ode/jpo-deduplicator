package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessageMessageFrame;
import us.dot.its.jpo.asn.j2735.r2024.Common.BSMcoreData;
import us.dot.its.jpo.asn.j2735.r2024.Common.Speed;
import us.dot.its.jpo.deduplicator.utils.GeoUtils;

public class OdeBsmJsonProcessor extends DeduplicationProcessor<OdeMessageFrameData> {

    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(OdeBsmJsonProcessor.class);

    public OdeBsmJsonProcessor(String storeName, DeduplicatorProperties props) {
        this.storeName = storeName;
        this.props = props;
    }

    @Override
    public Instant getMessageTime(OdeMessageFrameData message) {
        try {
            if (message == null || message.getMetadata() == null) {
                logger.warn("BSM message or metadata is null, using epoch time");
                return Instant.ofEpochMilli(0);
            }

            String time = ((OdeMessageFrameMetadata) message.getMetadata()).getOdeReceivedAt();
            if (time == null || time.isEmpty()) {
                logger.warn("BSM message has null or empty odeReceivedAt time, using epoch time");
                return Instant.ofEpochMilli(0);
            }

            return Instant.from(formatter.parse(time));
        } catch (Exception e) {
            logger.warn("Failed to Parse Time: " + (message != null && message.getMetadata() != null
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

            // If the messages are more than a certain time apart, forward the new message
            // on
            if (newValueTime.minus(Duration.ofMillis(props.getOdeBsmMaximumTimeDelta())).isAfter(oldValueTime)) {
                return false;
            }

            // Add null checks for payload and data
            if (lastMessage == null || lastMessage.getPayload() == null || lastMessage.getPayload().getData() == null ||
                    newMessage == null || newMessage.getPayload() == null
                    || newMessage.getPayload().getData() == null) {
                logger.warn("One or both BSM messages have null payload or data, treating as non-duplicate");
                return false;
            }

            BSMcoreData oldCore = ((BasicSafetyMessageMessageFrame) lastMessage.getPayload().getData()).getValue()
                    .getCoreData();
            BSMcoreData newCore = ((BasicSafetyMessageMessageFrame) newMessage.getPayload().getData()).getValue()
                    .getCoreData();

            // Check if speed availability status has changed (null, unavailable, or
            // available)
            boolean oldSpeedAvailable = isSpeedAvailable(oldCore.getSpeed());
            boolean newSpeedAvailable = isSpeedAvailable(newCore.getSpeed());

            // If the speed availability status has changed, forward the message
            if (oldSpeedAvailable != newSpeedAvailable) {
                return false;
            }

            // If the Vehicle is moving (speed is available and above threshold), forward
            // the message on
            if (newSpeedAvailable && newCore.getSpeed().getValue() > props.getOdeBsmAlwaysIncludeAtSpeed()) {
                return false;
            }

            // If the new core and the old core have different null conditions
            if (((oldCore.getLat() == null || oldCore.getLong_() == null)
                    // Used to be null, but now is non-null
                    && (newCore.getLat() != null || newCore.getLong_() != null)) ||
                    ((oldCore.getLat() != null || oldCore.getLong_() != null)
                            && (newCore.getLat() == null || newCore.getLong_() == null))) {
                return false;
                // both are null, message is a duplicate
            } else if (oldCore.getLat() == null && newCore.getLat() == null && oldCore.getLong_() == null
                    && newCore.getLong_() == null) {
                return true;
            } else {
                double distance = GeoUtils.calculateGeodeticDistanceJ2735(
                        newCore.getLat().getValue(),
                        newCore.getLong_().getValue(),
                        oldCore.getLat().getValue(),
                        oldCore.getLong_().getValue());

                // If the position delta between the messages is suitable large, forward the
                // message on
                if (distance > props.getOdeBsmMaximumPositionDelta()) {
                    return false;
                }
            }
        } catch (Exception e) {
            logger.warn("Caught General Exception" + e);
        }

        return true;
    }

    /**
     * Determines if the speed value is available (not null and not unavailable)
     * 
     * @param speed The speed object to check
     * @return true if speed is available, false otherwise
     */
    private boolean isSpeedAvailable(Speed speed) {
        if (speed == null) {
            return false;
        }
        // Speed value 8191 indicates unavailable in J2735 standard
        return speed.getValue() != 8191;
    }
}
