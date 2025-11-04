package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

public class ProcessedSpatProcessor extends DeduplicationProcessor<ProcessedSpat>{

    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(ProcessedSpatProcessor.class);

    public ProcessedSpatProcessor(DeduplicatorProperties props){
        this.props = props;
        this.storeName = props.getKafkaStateStoreProcessedSpatName();
    }

    @Override
    public Instant getMessageTime(ProcessedSpat message) {
        return message.getUtcTimeStamp().toInstant();
    }

    @Override
    public boolean isDuplicate(ProcessedSpat lastMessage, ProcessedSpat newMessage) {
        try{
            Instant newValueTime = getMessageTime(newMessage);
            Instant oldValueTime = getMessageTime(lastMessage);
            
            if(newValueTime.minus(Duration.ofMinutes(1)).isAfter(oldValueTime)) {
                return false;
            } else {
                HashMap<Integer, List<ProcessedMovementEvent>> lastMessageStates = new HashMap<>();
                for(ProcessedMovementState state: lastMessage.getStates()){
                    lastMessageStates.put(state.getSignalGroup(), state.getStateTimeSpeed());
                }

                if(lastMessageStates.size() != newMessage.getStates().size()){
                    return false; // message cannot be duplicate if the signal groups have a different number of signal groups
                }

                for(ProcessedMovementState state: newMessage.getStates()){
                    List<ProcessedMovementEvent> lastMessageState = lastMessageStates.get(state.getSignalGroup());

                    if(lastMessageState == null){
                        return false; // messages cannot be duplicates if they have different signal groups
                    }

                    
                    for(int i=0; i< state.getStateTimeSpeed().size(); i++){
                        if(state.getStateTimeSpeed().get(i).getEventState() != lastMessageState.get(i).getEventState()){
                            return false; // Some signal group light has changed. Therefore the SPaTs are different
                        }
                    }
                }
            }
        } catch(Exception e){
            logger.warn("Caught General Exception" + e);
        }
        return true;   
    }
}
