package us.dot.its.jpo.deduplicator.utils;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.Test;

public class GeoUtilsTest {

    private static final double DELTA = 50; // Acceptable difference for double comparisons

    @Test
    public void testLargeDistanceValidCoordinates() {
        double lat1 = 40.7000;
        double lon1 = -74.0000;
        double lat2 = 40.8000;
        double lon2 = -74.1000;
   
        double distance = GeoUtils.calculateGeodeticDistance(lat1, lon1, lat2, lon2);
        assertEquals(13951.0, distance, DELTA);
    }

    @Test
    public void testSmallDistanceValidCoordinates() {
        double lat1 = 40.7000;
        double lon1 = -74.0000;
        double lat2 = 40.7010;
        double lon2 = -74.0010;
   
        double distance = GeoUtils.calculateGeodeticDistance(lat1, lon1, lat2, lon2);
        assertEquals(139.0, distance, DELTA);
    }

    @Test
    public void testSamePoint() {
        double lat = 40.7128;
        double lon = -74.0060;
        
        double distance = GeoUtils.calculateGeodeticDistance(lat, lon, lat, lon);
        assertEquals(0.0, distance, DELTA);
    }

    @Test
    public void testInvalidLatitude() {
        double distance = GeoUtils.calculateGeodeticDistance(91.0, 0.0, 45.0, 0.0);
        assertEquals(-1, distance, DELTA);

        distance = GeoUtils.calculateGeodeticDistance(45.0, 0.0, -91.0, 0.0);
        assertEquals(-1, distance, DELTA);
    }

    @Test
    public void testInvalidLongitude() {
        double distance = GeoUtils.calculateGeodeticDistance(0.0, 181.0, 0.0, 0.0);
        assertEquals(-1, distance, DELTA);

        distance = GeoUtils.calculateGeodeticDistance(0.0, 0.0, 0.0, -181.0);
        assertEquals(-1, distance, DELTA);
    }

} 