package us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers;

import java.time.format.DateTimeFormatter;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.processors.ProcessedBsmJsonProcessor;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;

public class ProcessedBsmJsonProcessorSupplier implements ProcessorSupplier<String, ProcessedBsm<Point>, String, ProcessedBsm<Point>> {
    
    String storeName;
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    DeduplicatorProperties props;

    public ProcessedBsmJsonProcessorSupplier(String storeName, DeduplicatorProperties props){
        this.storeName = storeName;
        this.props = props;
    }

    @Override
    public Processor<String, ProcessedBsm<Point>, String, ProcessedBsm<Point>> get() {
        return new ProcessedBsmJsonProcessor(storeName, props);
    }
}