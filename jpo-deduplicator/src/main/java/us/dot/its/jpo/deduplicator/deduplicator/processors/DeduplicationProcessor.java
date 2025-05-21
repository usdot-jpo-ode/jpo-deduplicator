package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.KeyValue;

public abstract class DeduplicationProcessor<T> implements Processor<String, T, String, T>{

    private ProcessorContext<String, T> context;
    private KeyValueStore<String, T> store;
    public String storeName;

    private static final Logger logger = LoggerFactory.getLogger(DeduplicationProcessor.class);

    @Override
    public void init(ProcessorContext<String, T> context) {
        this.context = context;
        store = context.getStateStore(storeName);
        this.context.schedule(Duration.ofHours(1), PunctuationType.WALL_CLOCK_TIME, this::cleanupOldKeys);
    }

    @Override
    public void process(Record<String, T> record) {

        // Don't do anything if key is bad
        if(record.key().equals("")){
            return;
        }

        T lastRecord = store.get(record.key());
        if(lastRecord == null){
            store.put(record.key(), record.value());
            context.forward(record);
            return;
        }

        try{
            if(!isDuplicate(lastRecord, record.value())){
                store.put(record.key(), record.value());
                context.forward(record);
                return;
            }
        } catch(Exception e){
            logger.warn("Caught General Exception while Checking Duplicates" + e);
        }
        
    }

    private void cleanupOldKeys(final long timestamp) {
        try (KeyValueIterator<String, T> iterator = store.all()) {
            while (iterator.hasNext()) {
            
            KeyValue<String, T> record = iterator.next();
                // Delete any record more than 2 hours old.
                if(Instant.ofEpochMilli(timestamp).minusSeconds(2 * 60 * 60).isAfter(getMessageTime(record.value))){
                    store.delete(record.key);
                }
            }
        }
    }

    // returns an instant representing the time of the message
    public abstract Instant getMessageTime(T message);

    // returns if two messages are duplicates of one another
    public abstract boolean isDuplicate(T lastMessage, T newMessage);

}
