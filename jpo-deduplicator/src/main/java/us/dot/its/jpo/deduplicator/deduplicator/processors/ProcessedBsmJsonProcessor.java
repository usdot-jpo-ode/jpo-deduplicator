package us.dot.its.jpo.deduplicator.deduplicator.processors;


import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;
import us.dot.its.jpo.deduplicator.utils.GeoUtils;

public class ProcessedBsmJsonProcessor extends DeduplicationProcessor<ProcessedBsm<Point>>{

    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(ProcessedBsmJsonProcessor.class);

    public ProcessedBsmJsonProcessor(String storeName, DeduplicatorProperties props){
        this.storeName = storeName;
        this.props = props;
    }


    @Override
    public Instant getMessageTime(ProcessedBsm<Point> message) {
        ZonedDateTime time = message.getProperties().getTimeStamp();
        try {
            return Instant.from(time);
        } catch (Exception e) {
            logger.warn("Failed to Parse Time: " + time);
            return Instant.ofEpochMilli(0);
        }
    }

    @Override
    public boolean isDuplicate(ProcessedBsm<Point> lastMessage, ProcessedBsm<Point> newMessage) {
        Instant newValueTime = getMessageTime(newMessage);
        Instant oldValueTime = getMessageTime(lastMessage);

        // If the messages are more than a certain time apart, forward the new message on
        if(newValueTime.minus(Duration.ofMillis(props.getProcessedBsmMaximumTimeDelta())).isAfter(oldValueTime)){
            return false;  
        }

        // If the Vehicle is moving, forward the message on
        BigDecimal speed = newMessage.getProperties().getSpeed();
        if (speed != null && speed.doubleValue() > props.getProcessedBsmAlwaysIncludeAtSpeed()) {
            return false; 
        }

        double distance = GeoUtils.calculateGeodeticDistance(
            ((Point)newMessage.getGeometry()).getCoordinates()[1],
            ((Point)newMessage.getGeometry()).getCoordinates()[0],
            ((Point)lastMessage.getGeometry()).getCoordinates()[1],
            ((Point)lastMessage.getGeometry()).getCoordinates()[0]
        );

        // If the position delta between the messages is suitable large, forward the message on
        if(distance > props.getProcessedBsmMaximumPositionDelta()){
            return false;
        }

        return true;
    }
}
