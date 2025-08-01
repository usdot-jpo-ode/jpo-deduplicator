package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerInformationMessageFrame;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

public class OdeTimJsonProcessor extends DeduplicationProcessor<OdeMessageFrameData>{

    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    private static final Logger logger = LoggerFactory.getLogger(OdeTimJsonProcessor.class);

    DeduplicatorProperties props;
    public OdeTimJsonProcessor(DeduplicatorProperties props){
        this.props = props;
        this.storeName = props.getKafkaStateStoreOdeTimJsonName();
    }


    @Override
    public Instant getMessageTime(OdeMessageFrameData message) {
        try {
            if (message == null || message.getMetadata() == null) {
                logger.warn("TIM message or metadata is null, using epoch time");
                return Instant.ofEpochMilli(0);
            }

            String time = message.getMetadata().getOdeReceivedAt();
            if (time == null || time.isEmpty()) {
                logger.warn("TIM message has null or empty odeReceivedAt time, using epoch time");
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
        try{
            Instant oldValueTime = getMessageTime(lastMessage);
            Instant newValueTime = getMessageTime(newMessage);

            if(newValueTime.minus(Duration.ofHours(1)).isAfter(oldValueTime)){
                return false;
            }

            // Add null checks for payload and data
            if (lastMessage == null || lastMessage.getPayload() == null || lastMessage.getPayload().getData() == null ||
                    newMessage == null || newMessage.getPayload() == null
                    || newMessage.getPayload().getData() == null) {
                logger.warn("One or both TIM messages have null payload or data, treating as non-duplicate");
                return false;
            }

            // Check if the TIM packet IDs are the same
            TravelerInformationMessageFrame oldTim = (TravelerInformationMessageFrame) lastMessage.getPayload().getData();
            TravelerInformationMessageFrame newTim = (TravelerInformationMessageFrame) newMessage.getPayload().getData();
            String oldTimPacketId = oldTim.getValue().getPacketID().getValue();
            String newTimPacketId = newTim.getValue().getPacketID().getValue();

            if (!oldTimPacketId.equals(newTimPacketId)) {
                return false;
            }

            // Check if the message count of the new TIM is identical to the old TIM
            if (newTim.getValue().getMsgCnt().getValue() != oldTim.getValue().getMsgCnt().getValue()) {
                return false;
            }
        } catch(Exception e){
            logger.warn("Caught General Exception" + e);
        }
        return true;
    }
}
