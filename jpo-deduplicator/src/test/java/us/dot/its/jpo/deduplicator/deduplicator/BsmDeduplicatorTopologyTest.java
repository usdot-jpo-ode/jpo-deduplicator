package us.dot.its.jpo.deduplicator.deduplicator;



import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.BsmDeduplicatorTopology;
import us.dot.its.jpo.ode.model.OdeBsmData;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;


public class BsmDeduplicatorTopologyTest {

    String inputTopic = "topic.OdeBsmJson";
    String outputTopic = "topic.DeduplicatedOdeBsmJson";
    ObjectMapper objectMapper;

    String inputBsm1 = "{\"metadata\":{\"bsmSource\":\"EV\",\"logFileName\":\"\",\"recordType\":\"bsmTx\",\"securityResultCode\":\"success\",\"receivedMessageDetails\":{\"locationData\":{\"latitude\":\"unavailable\",\"longitude\":\"unavailable\",\"elevation\":\"unavailable\",\"speed\":\"unavailable\",\"heading\":\"unavailable\"},\"rxSource\":\"RSU\"},\"payloadType\":\"us.dot.its.jpo.ode.model.OdeBsmPayload\",\"serialId\":{\"streamId\":\"9bc470d1-babe-415b-8c5a-4cd4a9403350\",\"bundleSize\":1,\"bundleId\":0,\"recordId\":0,\"serialNumber\":0},\"odeReceivedAt\":\"2025-01-31T23:14:24.693Z\",\"schemaVersion\":8,\"maxDurationTime\":0,\"recordGeneratedAt\":\"\",\"recordGeneratedBy\":\"OBU\",\"sanitized\":false,\"odePacketID\":\"\",\"odeTimStartDateTime\":\"\",\"asn1\":\"001480B8494C4C950CD8CDE6E9651116579F22A424DD78FFFFF00761E4FD7EB7D07F7FFF80005F11D1020214C1C0FFC7C016AFF4017A0FF65403B0FD204C20FFCCC04F8FE40C420FFE6404CEFE60E9A10133408FCFDE1438103AB4138F00E1EEC1048EC160103E237410445C171104E26BC103DC4154305C2C84103B1C1C8F0A82F42103F34262D1123198103DAC25FB12034CE10381C259F12038CA103574251B10E3B2210324C23AD0F23D8EFFFE0000209340D10000004264BF00\",\"originIp\":\"172.20.0.1\"},\"payload\":{\"data\":{\"coreData\":{\"msgCnt\":37,\"id\":\"31325433\",\"secMark\":25399,\"position\":{\"latitude\":40.5659938,\"longitude\":-105.0317754,\"elevation\":1440.9},\"accelSet\":{\"accelLat\":0.00,\"accelLong\":0.27,\"accelVert\":0.00,\"accelYaw\":0.00},\"accuracy\":{\"semiMajor\":9.30,\"semiMinor\":12.05},\"transmission\":\"UNAVAILABLE\",\"speed\":0.28,\"heading\":313.2500,\"brakes\":{\"wheelBrakes\":{\"leftFront\":false,\"rightFront\":false,\"unavailable\":true,\"leftRear\":false,\"rightRear\":false},\"traction\":\"unavailable\",\"abs\":\"unavailable\",\"scs\":\"unavailable\",\"brakeBoost\":\"unavailable\",\"auxBrakes\":\"unavailable\"},\"size\":{\"width\":190,\"length\":570}},\"partII\":[{\"id\":\"VehicleSafetyExtensions\",\"value\":{\"pathHistory\":{\"crumbData\":[{\"elevationOffset\":-0.6,\"latOffset\":-0.0000113,\"lonOffset\":0.0000181,\"timeOffset\":1.90},{\"elevationOffset\":-2.3,\"latOffset\":-0.0000310,\"lonOffset\":0.0000472,\"timeOffset\":6.10},{\"elevationOffset\":-1.4,\"latOffset\":-0.0000103,\"lonOffset\":0.0000636,\"timeOffset\":15.70},{\"elevationOffset\":-1.3,\"latOffset\":-0.0000052,\"lonOffset\":0.0000615,\"timeOffset\":18.70},{\"elevationOffset\":-1.7,\"latOffset\":0.0000614,\"lonOffset\":0.0001150,\"timeOffset\":25.89},{\"elevationOffset\":0.7,\"latOffset\":0.0001878,\"lonOffset\":0.0002503,\"timeOffset\":39.59},{\"elevationOffset\":3.1,\"latOffset\":0.0002333,\"lonOffset\":0.0002816,\"timeOffset\":45.39},{\"elevationOffset\":3.9,\"latOffset\":0.0002187,\"lonOffset\":0.0002952,\"timeOffset\":49.59},{\"elevationOffset\":4.6,\"latOffset\":0.0001976,\"lonOffset\":0.0002721,\"timeOffset\":56.99},{\"elevationOffset\":8.4,\"latOffset\":0.0001891,\"lonOffset\":0.0003655,\"timeOffset\":60.50},{\"elevationOffset\":13.7,\"latOffset\":0.0002022,\"lonOffset\":0.0004886,\"timeOffset\":63.49},{\"elevationOffset\":14.4,\"latOffset\":0.0001973,\"lonOffset\":0.0004861,\"timeOffset\":67.60},{\"elevationOffset\":14.4,\"latOffset\":0.0001795,\"lonOffset\":0.0004815,\"timeOffset\":72.70},{\"elevationOffset\":13.5,\"latOffset\":0.0001710,\"lonOffset\":0.0004749,\"timeOffset\":75.70},{\"elevationOffset\":12.1,\"latOffset\":0.0001609,\"lonOffset\":0.0004566,\"timeOffset\":78.80}]},\"pathPrediction\":{\"confidence\":0.0,\"radiusOfCurve\":0.0}}},{\"id\":\"SupplementalVehicleExtensions\",\"value\":{\"classDetails\":{\"fuelType\":\"unknownFuel\",\"hpmsType\":\"none\",\"keyType\":0,\"role\":\"basicVehicle\"},\"vehicleData\":{\"height\":1.90},\"doNotUse2\":{}}}]},\"dataType\":\"us.dot.its.jpo.ode.plugin.j2735.J2735Bsm\"}}";
    
    // Same as BSM 1 - No Message should be generated
    String inputBsm2 = "{\"metadata\":{\"bsmSource\":\"EV\",\"logFileName\":\"\",\"recordType\":\"bsmTx\",\"securityResultCode\":\"success\",\"receivedMessageDetails\":{\"locationData\":{\"latitude\":\"unavailable\",\"longitude\":\"unavailable\",\"elevation\":\"unavailable\",\"speed\":\"unavailable\",\"heading\":\"unavailable\"},\"rxSource\":\"RSU\"},\"payloadType\":\"us.dot.its.jpo.ode.model.OdeBsmPayload\",\"serialId\":{\"streamId\":\"9bc470d1-babe-415b-8c5a-4cd4a9403350\",\"bundleSize\":1,\"bundleId\":0,\"recordId\":0,\"serialNumber\":0},\"odeReceivedAt\":\"2025-01-31T23:14:24.793Z\",\"schemaVersion\":8,\"maxDurationTime\":0,\"recordGeneratedAt\":\"\",\"recordGeneratedBy\":\"OBU\",\"sanitized\":false,\"odePacketID\":\"\",\"odeTimStartDateTime\":\"\",\"asn1\":\"001480B8494C4C950CD8CDE6E9651116579F22A424DD78FFFFF00761E4FD7EB7D07F7FFF80005F11D1020214C1C0FFC7C016AFF4017A0FF65403B0FD204C20FFCCC04F8FE40C420FFE6404CEFE60E9A10133408FCFDE1438103AB4138F00E1EEC1048EC160103E237410445C171104E26BC103DC4154305C2C84103B1C1C8F0A82F42103F34262D1123198103DAC25FB12034CE10381C259F12038CA103574251B10E3B2210324C23AD0F23D8EFFFE0000209340D10000004264BF00\",\"originIp\":\"172.20.0.1\"},\"payload\":{\"data\":{\"coreData\":{\"msgCnt\":37,\"id\":\"31325433\",\"secMark\":25399,\"position\":{\"latitude\":40.5659938,\"longitude\":-105.0317754,\"elevation\":1440.9},\"accelSet\":{\"accelLat\":0.00,\"accelLong\":0.27,\"accelVert\":0.00,\"accelYaw\":0.00},\"accuracy\":{\"semiMajor\":9.30,\"semiMinor\":12.05},\"transmission\":\"UNAVAILABLE\",\"speed\":0.28,\"heading\":313.2500,\"brakes\":{\"wheelBrakes\":{\"leftFront\":false,\"rightFront\":false,\"unavailable\":true,\"leftRear\":false,\"rightRear\":false},\"traction\":\"unavailable\",\"abs\":\"unavailable\",\"scs\":\"unavailable\",\"brakeBoost\":\"unavailable\",\"auxBrakes\":\"unavailable\"},\"size\":{\"width\":190,\"length\":570}},\"partII\":[{\"id\":\"VehicleSafetyExtensions\",\"value\":{\"pathHistory\":{\"crumbData\":[{\"elevationOffset\":-0.6,\"latOffset\":-0.0000113,\"lonOffset\":0.0000181,\"timeOffset\":1.90},{\"elevationOffset\":-2.3,\"latOffset\":-0.0000310,\"lonOffset\":0.0000472,\"timeOffset\":6.10},{\"elevationOffset\":-1.4,\"latOffset\":-0.0000103,\"lonOffset\":0.0000636,\"timeOffset\":15.70},{\"elevationOffset\":-1.3,\"latOffset\":-0.0000052,\"lonOffset\":0.0000615,\"timeOffset\":18.70},{\"elevationOffset\":-1.7,\"latOffset\":0.0000614,\"lonOffset\":0.0001150,\"timeOffset\":25.89},{\"elevationOffset\":0.7,\"latOffset\":0.0001878,\"lonOffset\":0.0002503,\"timeOffset\":39.59},{\"elevationOffset\":3.1,\"latOffset\":0.0002333,\"lonOffset\":0.0002816,\"timeOffset\":45.39},{\"elevationOffset\":3.9,\"latOffset\":0.0002187,\"lonOffset\":0.0002952,\"timeOffset\":49.59},{\"elevationOffset\":4.6,\"latOffset\":0.0001976,\"lonOffset\":0.0002721,\"timeOffset\":56.99},{\"elevationOffset\":8.4,\"latOffset\":0.0001891,\"lonOffset\":0.0003655,\"timeOffset\":60.50},{\"elevationOffset\":13.7,\"latOffset\":0.0002022,\"lonOffset\":0.0004886,\"timeOffset\":63.49},{\"elevationOffset\":14.4,\"latOffset\":0.0001973,\"lonOffset\":0.0004861,\"timeOffset\":67.60},{\"elevationOffset\":14.4,\"latOffset\":0.0001795,\"lonOffset\":0.0004815,\"timeOffset\":72.70},{\"elevationOffset\":13.5,\"latOffset\":0.0001710,\"lonOffset\":0.0004749,\"timeOffset\":75.70},{\"elevationOffset\":12.1,\"latOffset\":0.0001609,\"lonOffset\":0.0004566,\"timeOffset\":78.80}]},\"pathPrediction\":{\"confidence\":0.0,\"radiusOfCurve\":0.0}}},{\"id\":\"SupplementalVehicleExtensions\",\"value\":{\"classDetails\":{\"fuelType\":\"unknownFuel\",\"hpmsType\":\"none\",\"keyType\":0,\"role\":\"basicVehicle\"},\"vehicleData\":{\"height\":1.90},\"doNotUse2\":{}}}]},\"dataType\":\"us.dot.its.jpo.ode.plugin.j2735.J2735Bsm\"}}";
    
    // Increase Time from Bsm 1
    String inputBsm3 = "{\"metadata\":{\"bsmSource\":\"EV\",\"logFileName\":\"\",\"recordType\":\"bsmTx\",\"securityResultCode\":\"success\",\"receivedMessageDetails\":{\"locationData\":{\"latitude\":\"unavailable\",\"longitude\":\"unavailable\",\"elevation\":\"unavailable\",\"speed\":\"unavailable\",\"heading\":\"unavailable\"},\"rxSource\":\"RSU\"},\"payloadType\":\"us.dot.its.jpo.ode.model.OdeBsmPayload\",\"serialId\":{\"streamId\":\"9bc470d1-babe-415b-8c5a-4cd4a9403350\",\"bundleSize\":1,\"bundleId\":0,\"recordId\":0,\"serialNumber\":0},\"odeReceivedAt\":\"2025-01-31T23:14:34.793Z\",\"schemaVersion\":8,\"maxDurationTime\":0,\"recordGeneratedAt\":\"\",\"recordGeneratedBy\":\"OBU\",\"sanitized\":false,\"odePacketID\":\"\",\"odeTimStartDateTime\":\"\",\"asn1\":\"001480B8494C4C950CD8CDE6E9651116579F22A424DD78FFFFF00761E4FD7EB7D07F7FFF80005F11D1020214C1C0FFC7C016AFF4017A0FF65403B0FD204C20FFCCC04F8FE40C420FFE6404CEFE60E9A10133408FCFDE1438103AB4138F00E1EEC1048EC160103E237410445C171104E26BC103DC4154305C2C84103B1C1C8F0A82F42103F34262D1123198103DAC25FB12034CE10381C259F12038CA103574251B10E3B2210324C23AD0F23D8EFFFE0000209340D10000004264BF00\",\"originIp\":\"172.20.0.1\"},\"payload\":{\"data\":{\"coreData\":{\"msgCnt\":37,\"id\":\"31325433\",\"secMark\":25399,\"position\":{\"latitude\":40.5659938,\"longitude\":-105.0317754,\"elevation\":1440.9},\"accelSet\":{\"accelLat\":0.00,\"accelLong\":0.27,\"accelVert\":0.00,\"accelYaw\":0.00},\"accuracy\":{\"semiMajor\":9.30,\"semiMinor\":12.05},\"transmission\":\"UNAVAILABLE\",\"speed\":0.28,\"heading\":313.2500,\"brakes\":{\"wheelBrakes\":{\"leftFront\":false,\"rightFront\":false,\"unavailable\":true,\"leftRear\":false,\"rightRear\":false},\"traction\":\"unavailable\",\"abs\":\"unavailable\",\"scs\":\"unavailable\",\"brakeBoost\":\"unavailable\",\"auxBrakes\":\"unavailable\"},\"size\":{\"width\":190,\"length\":570}},\"partII\":[{\"id\":\"VehicleSafetyExtensions\",\"value\":{\"pathHistory\":{\"crumbData\":[{\"elevationOffset\":-0.6,\"latOffset\":-0.0000113,\"lonOffset\":0.0000181,\"timeOffset\":1.90},{\"elevationOffset\":-2.3,\"latOffset\":-0.0000310,\"lonOffset\":0.0000472,\"timeOffset\":6.10},{\"elevationOffset\":-1.4,\"latOffset\":-0.0000103,\"lonOffset\":0.0000636,\"timeOffset\":15.70},{\"elevationOffset\":-1.3,\"latOffset\":-0.0000052,\"lonOffset\":0.0000615,\"timeOffset\":18.70},{\"elevationOffset\":-1.7,\"latOffset\":0.0000614,\"lonOffset\":0.0001150,\"timeOffset\":25.89},{\"elevationOffset\":0.7,\"latOffset\":0.0001878,\"lonOffset\":0.0002503,\"timeOffset\":39.59},{\"elevationOffset\":3.1,\"latOffset\":0.0002333,\"lonOffset\":0.0002816,\"timeOffset\":45.39},{\"elevationOffset\":3.9,\"latOffset\":0.0002187,\"lonOffset\":0.0002952,\"timeOffset\":49.59},{\"elevationOffset\":4.6,\"latOffset\":0.0001976,\"lonOffset\":0.0002721,\"timeOffset\":56.99},{\"elevationOffset\":8.4,\"latOffset\":0.0001891,\"lonOffset\":0.0003655,\"timeOffset\":60.50},{\"elevationOffset\":13.7,\"latOffset\":0.0002022,\"lonOffset\":0.0004886,\"timeOffset\":63.49},{\"elevationOffset\":14.4,\"latOffset\":0.0001973,\"lonOffset\":0.0004861,\"timeOffset\":67.60},{\"elevationOffset\":14.4,\"latOffset\":0.0001795,\"lonOffset\":0.0004815,\"timeOffset\":72.70},{\"elevationOffset\":13.5,\"latOffset\":0.0001710,\"lonOffset\":0.0004749,\"timeOffset\":75.70},{\"elevationOffset\":12.1,\"latOffset\":0.0001609,\"lonOffset\":0.0004566,\"timeOffset\":78.80}]},\"pathPrediction\":{\"confidence\":0.0,\"radiusOfCurve\":0.0}}},{\"id\":\"SupplementalVehicleExtensions\",\"value\":{\"classDetails\":{\"fuelType\":\"unknownFuel\",\"hpmsType\":\"none\",\"keyType\":0,\"role\":\"basicVehicle\"},\"vehicleData\":{\"height\":1.90},\"doNotUse2\":{}}}]},\"dataType\":\"us.dot.its.jpo.ode.plugin.j2735.J2735Bsm\"}}";
    
    // Vehicle Speed not 0
    String inputBsm4 = "{\"metadata\":{\"bsmSource\":\"EV\",\"logFileName\":\"\",\"recordType\":\"bsmTx\",\"securityResultCode\":\"success\",\"receivedMessageDetails\":{\"locationData\":{\"latitude\":\"unavailable\",\"longitude\":\"unavailable\",\"elevation\":\"unavailable\",\"speed\":\"unavailable\",\"heading\":\"unavailable\"},\"rxSource\":\"RSU\"},\"payloadType\":\"us.dot.its.jpo.ode.model.OdeBsmPayload\",\"serialId\":{\"streamId\":\"9bc470d1-babe-415b-8c5a-4cd4a9403350\",\"bundleSize\":1,\"bundleId\":0,\"recordId\":0,\"serialNumber\":0},\"odeReceivedAt\":\"2025-01-31T23:14:34.893Z\",\"schemaVersion\":8,\"maxDurationTime\":0,\"recordGeneratedAt\":\"\",\"recordGeneratedBy\":\"OBU\",\"sanitized\":false,\"odePacketID\":\"\",\"odeTimStartDateTime\":\"\",\"asn1\":\"001480B8494C4C950CD8CDE6E9651116579F22A424DD78FFFFF00761E4FD7EB7D07F7FFF80005F11D1020214C1C0FFC7C016AFF4017A0FF65403B0FD204C20FFCCC04F8FE40C420FFE6404CEFE60E9A10133408FCFDE1438103AB4138F00E1EEC1048EC160103E237410445C171104E26BC103DC4154305C2C84103B1C1C8F0A82F42103F34262D1123198103DAC25FB12034CE10381C259F12038CA103574251B10E3B2210324C23AD0F23D8EFFFE0000209340D10000004264BF00\",\"originIp\":\"172.20.0.1\"},\"payload\":{\"data\":{\"coreData\":{\"msgCnt\":37,\"id\":\"31325433\",\"secMark\":25399,\"position\":{\"latitude\":40.5659938,\"longitude\":-105.0317754,\"elevation\":1440.9},\"accelSet\":{\"accelLat\":0.00,\"accelLong\":0.27,\"accelVert\":0.00,\"accelYaw\":0.00},\"accuracy\":{\"semiMajor\":9.30,\"semiMinor\":12.05},\"transmission\":\"UNAVAILABLE\",\"speed\":5,\"heading\":313.2500,\"brakes\":{\"wheelBrakes\":{\"leftFront\":false,\"rightFront\":false,\"unavailable\":true,\"leftRear\":false,\"rightRear\":false},\"traction\":\"unavailable\",\"abs\":\"unavailable\",\"scs\":\"unavailable\",\"brakeBoost\":\"unavailable\",\"auxBrakes\":\"unavailable\"},\"size\":{\"width\":190,\"length\":570}},\"partII\":[{\"id\":\"VehicleSafetyExtensions\",\"value\":{\"pathHistory\":{\"crumbData\":[{\"elevationOffset\":-0.6,\"latOffset\":-0.0000113,\"lonOffset\":0.0000181,\"timeOffset\":1.90},{\"elevationOffset\":-2.3,\"latOffset\":-0.0000310,\"lonOffset\":0.0000472,\"timeOffset\":6.10},{\"elevationOffset\":-1.4,\"latOffset\":-0.0000103,\"lonOffset\":0.0000636,\"timeOffset\":15.70},{\"elevationOffset\":-1.3,\"latOffset\":-0.0000052,\"lonOffset\":0.0000615,\"timeOffset\":18.70},{\"elevationOffset\":-1.7,\"latOffset\":0.0000614,\"lonOffset\":0.0001150,\"timeOffset\":25.89},{\"elevationOffset\":0.7,\"latOffset\":0.0001878,\"lonOffset\":0.0002503,\"timeOffset\":39.59},{\"elevationOffset\":3.1,\"latOffset\":0.0002333,\"lonOffset\":0.0002816,\"timeOffset\":45.39},{\"elevationOffset\":3.9,\"latOffset\":0.0002187,\"lonOffset\":0.0002952,\"timeOffset\":49.59},{\"elevationOffset\":4.6,\"latOffset\":0.0001976,\"lonOffset\":0.0002721,\"timeOffset\":56.99},{\"elevationOffset\":8.4,\"latOffset\":0.0001891,\"lonOffset\":0.0003655,\"timeOffset\":60.50},{\"elevationOffset\":13.7,\"latOffset\":0.0002022,\"lonOffset\":0.0004886,\"timeOffset\":63.49},{\"elevationOffset\":14.4,\"latOffset\":0.0001973,\"lonOffset\":0.0004861,\"timeOffset\":67.60},{\"elevationOffset\":14.4,\"latOffset\":0.0001795,\"lonOffset\":0.0004815,\"timeOffset\":72.70},{\"elevationOffset\":13.5,\"latOffset\":0.0001710,\"lonOffset\":0.0004749,\"timeOffset\":75.70},{\"elevationOffset\":12.1,\"latOffset\":0.0001609,\"lonOffset\":0.0004566,\"timeOffset\":78.80}]},\"pathPrediction\":{\"confidence\":0.0,\"radiusOfCurve\":0.0}}},{\"id\":\"SupplementalVehicleExtensions\",\"value\":{\"classDetails\":{\"fuelType\":\"unknownFuel\",\"hpmsType\":\"none\",\"keyType\":0,\"role\":\"basicVehicle\"},\"vehicleData\":{\"height\":1.90},\"doNotUse2\":{}}}]},\"dataType\":\"us.dot.its.jpo.ode.plugin.j2735.J2735Bsm\"}}";

    // Vehicle Position has changed 
    String inputBsm5 = "{\"metadata\":{\"bsmSource\":\"EV\",\"logFileName\":\"\",\"recordType\":\"bsmTx\",\"securityResultCode\":\"success\",\"receivedMessageDetails\":{\"locationData\":{\"latitude\":\"unavailable\",\"longitude\":\"unavailable\",\"elevation\":\"unavailable\",\"speed\":\"unavailable\",\"heading\":\"unavailable\"},\"rxSource\":\"RSU\"},\"payloadType\":\"us.dot.its.jpo.ode.model.OdeBsmPayload\",\"serialId\":{\"streamId\":\"9bc470d1-babe-415b-8c5a-4cd4a9403350\",\"bundleSize\":1,\"bundleId\":0,\"recordId\":0,\"serialNumber\":0},\"odeReceivedAt\":\"2025-01-31T23:14:34.993Z\",\"schemaVersion\":8,\"maxDurationTime\":0,\"recordGeneratedAt\":\"\",\"recordGeneratedBy\":\"OBU\",\"sanitized\":false,\"odePacketID\":\"\",\"odeTimStartDateTime\":\"\",\"asn1\":\"001480B8494C4C950CD8CDE6E9651116579F22A424DD78FFFFF00761E4FD7EB7D07F7FFF80005F11D1020214C1C0FFC7C016AFF4017A0FF65403B0FD204C20FFCCC04F8FE40C420FFE6404CEFE60E9A10133408FCFDE1438103AB4138F00E1EEC1048EC160103E237410445C171104E26BC103DC4154305C2C84103B1C1C8F0A82F42103F34262D1123198103DAC25FB12034CE10381C259F12038CA103574251B10E3B2210324C23AD0F23D8EFFFE0000209340D10000004264BF00\",\"originIp\":\"172.20.0.1\"},\"payload\":{\"data\":{\"coreData\":{\"msgCnt\":37,\"id\":\"31325433\",\"secMark\":25399,\"position\":{\"latitude\":40.6659938,\"longitude\":-105.0317754,\"elevation\":1440.9},\"accelSet\":{\"accelLat\":0.00,\"accelLong\":0.27,\"accelVert\":0.00,\"accelYaw\":0.00},\"accuracy\":{\"semiMajor\":9.30,\"semiMinor\":12.05},\"transmission\":\"UNAVAILABLE\",\"speed\":0.28,\"heading\":313.2500,\"brakes\":{\"wheelBrakes\":{\"leftFront\":false,\"rightFront\":false,\"unavailable\":true,\"leftRear\":false,\"rightRear\":false},\"traction\":\"unavailable\",\"abs\":\"unavailable\",\"scs\":\"unavailable\",\"brakeBoost\":\"unavailable\",\"auxBrakes\":\"unavailable\"},\"size\":{\"width\":190,\"length\":570}},\"partII\":[{\"id\":\"VehicleSafetyExtensions\",\"value\":{\"pathHistory\":{\"crumbData\":[{\"elevationOffset\":-0.6,\"latOffset\":-0.0000113,\"lonOffset\":0.0000181,\"timeOffset\":1.90},{\"elevationOffset\":-2.3,\"latOffset\":-0.0000310,\"lonOffset\":0.0000472,\"timeOffset\":6.10},{\"elevationOffset\":-1.4,\"latOffset\":-0.0000103,\"lonOffset\":0.0000636,\"timeOffset\":15.70},{\"elevationOffset\":-1.3,\"latOffset\":-0.0000052,\"lonOffset\":0.0000615,\"timeOffset\":18.70},{\"elevationOffset\":-1.7,\"latOffset\":0.0000614,\"lonOffset\":0.0001150,\"timeOffset\":25.89},{\"elevationOffset\":0.7,\"latOffset\":0.0001878,\"lonOffset\":0.0002503,\"timeOffset\":39.59},{\"elevationOffset\":3.1,\"latOffset\":0.0002333,\"lonOffset\":0.0002816,\"timeOffset\":45.39},{\"elevationOffset\":3.9,\"latOffset\":0.0002187,\"lonOffset\":0.0002952,\"timeOffset\":49.59},{\"elevationOffset\":4.6,\"latOffset\":0.0001976,\"lonOffset\":0.0002721,\"timeOffset\":56.99},{\"elevationOffset\":8.4,\"latOffset\":0.0001891,\"lonOffset\":0.0003655,\"timeOffset\":60.50},{\"elevationOffset\":13.7,\"latOffset\":0.0002022,\"lonOffset\":0.0004886,\"timeOffset\":63.49},{\"elevationOffset\":14.4,\"latOffset\":0.0001973,\"lonOffset\":0.0004861,\"timeOffset\":67.60},{\"elevationOffset\":14.4,\"latOffset\":0.0001795,\"lonOffset\":0.0004815,\"timeOffset\":72.70},{\"elevationOffset\":13.5,\"latOffset\":0.0001710,\"lonOffset\":0.0004749,\"timeOffset\":75.70},{\"elevationOffset\":12.1,\"latOffset\":0.0001609,\"lonOffset\":0.0004566,\"timeOffset\":78.80}]},\"pathPrediction\":{\"confidence\":0.0,\"radiusOfCurve\":0.0}}},{\"id\":\"SupplementalVehicleExtensions\",\"value\":{\"classDetails\":{\"fuelType\":\"unknownFuel\",\"hpmsType\":\"none\",\"keyType\":0,\"role\":\"basicVehicle\"},\"vehicleData\":{\"height\":1.90},\"doNotUse2\":{}}}]},\"dataType\":\"us.dot.its.jpo.ode.plugin.j2735.J2735Bsm\"}}";
    
    
    @Autowired
    DeduplicatorProperties props;

    

    @Test
    public void testTopology() {

        

        

        props = new DeduplicatorProperties();
        props.setAlwaysIncludeAtSpeed(1);
        props.setenableOdeBsmDeduplication(true);
        props.setMaximumPositionDelta(1);
        props.setMaximumTimeDelta(10000);
        props.setkafkaTopicOdeBsmJson(inputTopic);
        props.setkafkaTopicDeduplicatedOdeBsmJson(outputTopic);

        BsmDeduplicatorTopology bsmDeduplicatorTopology = new BsmDeduplicatorTopology(props);
        Topology topology = bsmDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            
            
            TestInputTopic<Void, String> inputOdeBsmData = driver.createInputTopic(
                inputTopic, 
                Serdes.Void().serializer(), 
                Serdes.String().serializer());


            TestOutputTopic<String, OdeBsmData> outputOdeBsmData = driver.createOutputTopic(
                outputTopic, 
                Serdes.String().deserializer(), 
                JsonSerdes.OdeBsm().deserializer());

            inputOdeBsmData.pipeInput(null, inputBsm1);
            inputOdeBsmData.pipeInput(null, inputBsm2);
            inputOdeBsmData.pipeInput(null, inputBsm3);
            inputOdeBsmData.pipeInput(null, inputBsm4);
            inputOdeBsmData.pipeInput(null, inputBsm5);

            List<KeyValue<String, OdeBsmData>> bsmDeduplicationResults = outputOdeBsmData.readKeyValuesToList();

            // validate that only 3 messages make it through
            assertEquals(4, bsmDeduplicationResults.size());

            objectMapper = new ObjectMapper();
            OdeBsmData bsm1 = objectMapper.readValue(inputBsm1, OdeBsmData.class);
            OdeBsmData bsm3 = objectMapper.readValue(inputBsm3, OdeBsmData.class);
            OdeBsmData bsm4 = objectMapper.readValue(inputBsm4, OdeBsmData.class);
            OdeBsmData bsm5 = objectMapper.readValue(inputBsm5, OdeBsmData.class);


            assertEquals(bsm1.getMetadata().getOdeReceivedAt(), bsmDeduplicationResults.get(0).value.getMetadata().getOdeReceivedAt());
            assertEquals(bsm3.getMetadata().getOdeReceivedAt(), bsmDeduplicationResults.get(1).value.getMetadata().getOdeReceivedAt());
            assertEquals(bsm4.getMetadata().getOdeReceivedAt(), bsmDeduplicationResults.get(2).value.getMetadata().getOdeReceivedAt());
            assertEquals(bsm5.getMetadata().getOdeReceivedAt(), bsmDeduplicationResults.get(3).value.getMetadata().getOdeReceivedAt());
           
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}