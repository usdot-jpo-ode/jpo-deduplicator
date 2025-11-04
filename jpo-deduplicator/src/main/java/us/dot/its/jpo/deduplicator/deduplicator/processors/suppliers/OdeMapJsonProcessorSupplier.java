package us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.processors.OdeMapJsonProcessor;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

public class OdeMapJsonProcessorSupplier
        implements ProcessorSupplier<String, OdeMessageFrameData, String, OdeMessageFrameData> {

    private String storeName;
    private DeduplicatorProperties props;

    public OdeMapJsonProcessorSupplier(String storeName, DeduplicatorProperties props) {
        this.storeName = storeName;
        this.props = props;
    }

    @Override
    public Processor<String, OdeMessageFrameData, String, OdeMessageFrameData> get() {
        return new OdeMapJsonProcessor(storeName, props);
    }
}