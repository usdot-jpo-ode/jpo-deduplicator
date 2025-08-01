package us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.processors.OdeTimJsonProcessor;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

public class OdeTimJsonProcessorSupplier implements ProcessorSupplier<String, OdeMessageFrameData, String, OdeMessageFrameData> {
    
    String storeName;
    DeduplicatorProperties props;
    public OdeTimJsonProcessorSupplier(DeduplicatorProperties props){
        this.props = props;
    }

    @Override
    public Processor<String, OdeMessageFrameData, String, OdeMessageFrameData> get() {
        return new OdeTimJsonProcessor(props);
    }
}