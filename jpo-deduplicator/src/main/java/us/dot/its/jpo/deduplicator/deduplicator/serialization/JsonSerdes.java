package us.dot.its.jpo.deduplicator.deduplicator.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;

public class JsonSerdes {

    private static final Logger logger = LoggerFactory.getLogger(JsonSerdes.class);

    /**
     * A wrapper deserializer that handles deserialization errors gracefully
     */
    private static class SafeJsonDeserializer<T> implements Deserializer<T> {
        private final JsonDeserializer<T> delegate;

        public SafeJsonDeserializer(Class<T> clazz) {
            this.delegate = new JsonDeserializer<>(clazz);
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return delegate.deserialize(topic, data);
            } catch (Exception e) {
                logger.warn("Failed to deserialize data for topic {}: {}", topic, e.getMessage());
                return null;
            }
        }
    }

    public static Serde<OdeMessageFrameData> OdeMessageFrame(DeduplicatorProperties props) {
        return Serdes.serdeFrom(
                new JsonSerializer<OdeMessageFrameData>(),
                new SafeJsonDeserializer<>(OdeMessageFrameData.class));
    }

    public static Serde<OdeMessageFrameData> OdeMessageFrame() {
        return OdeMessageFrame(null);
    }
}
