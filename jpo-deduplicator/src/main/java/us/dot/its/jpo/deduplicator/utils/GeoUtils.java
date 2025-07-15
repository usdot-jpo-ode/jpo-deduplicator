package us.dot.its.jpo.deduplicator.utils;

import org.geotools.referencing.GeodeticCalculator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeoUtils {
    private static final GeodeticCalculator calculator = new GeodeticCalculator();
    private static final double J2735_SCALE_FACTOR = 10000000; // 10000000 = 1 meter

    /**
     * Calculates the geodetic distance between two points on Earth
     * 
     * @param lat1 Latitude of first point (-90 to 90)
     * @param lon1 Longitude of first point (-180 to 180)
     * @param lat2 Latitude of second point (-90 to 90)
     * @param lon2 Longitude of second point (-180 to 180)
     * @return Distance in meters between the two points, or -1 if coordinates are
     *         invalid
     */
    public static double calculateGeodeticDistance(double lat1, double lon1, double lat2, double lon2) {
        try {
            if (lat1 < -90 || lat1 > 90 || lat2 < -90 || lat2 > 90) {
                log.error("Invalid latitude value(s). Latitude must be between -90 and 90 degrees.");
                return -1;
            }

            if (lon1 < -180 || lon1 > 180 || lon2 < -180 || lon2 > 180) {
                log.error("Invalid longitude value(s). Longitude must be between -180 and 180 degrees.");
                return -1;
            }

            calculator.setStartingGeographicPoint(lon1, lat1);
            calculator.setDestinationGeographicPoint(lon2, lat2);
            return calculator.getOrthodromicDistance();
        } catch (Exception e) {
            log.error("Error calculating geodetic distance: {}", e.getMessage());
            return -1;
        }

    }

    /**
     * Calculates the geodetic distance between two points on Earth with J2735 scale
     * factor
     * 
     * @param lat1 Latitude of first point (-900000000 to 900000000)
     * @param lon1 Longitude of first point (-1800000000 to 1800000000)
     * @param lat2 Latitude of second point (-900000000 to 900000000)
     * @param lon2 Longitude of second point (-1800000000 to 1800000000)
     * @return Distance in meters between the two points, or -1 if coordinates are
     *         invalid
     */
    public static double calculateGeodeticDistanceJ2735(double lat1, double lon1, double lat2, double lon2) {
        return calculateGeodeticDistance(lat1 / J2735_SCALE_FACTOR, lon1 / J2735_SCALE_FACTOR,
                lat2 / J2735_SCALE_FACTOR, lon2 / J2735_SCALE_FACTOR);
    }
}
