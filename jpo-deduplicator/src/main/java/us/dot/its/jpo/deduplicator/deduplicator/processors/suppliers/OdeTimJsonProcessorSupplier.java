package us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.processors.OdeTimJsonProcessor;
import us.dot.its.jpo.ode.model.OdeTimData;

public class OdeTimJsonProcessorSupplier implements ProcessorSupplier<String, OdeTimData, String, OdeTimData> {
    
    String storeName;
    DeduplicatorProperties props;
    public OdeTimJsonProcessorSupplier(DeduplicatorProperties props){
        this.props = props;
    }

    @Override
    public Processor<String, OdeTimData, String, OdeTimData> get() {
        return new OdeTimJsonProcessor(props);
    }
}