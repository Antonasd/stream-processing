package KubeScale.Events.Serdes;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import KubeScale.Events.Event;
import KubeScale.Events.JsonPOJOSerializer;

public class EventSerde<T> implements Serde<Event<T>>{
	private Serializer<Event<T>> serializer;
	private Deserializer<Event<T>> deserializer;
	
	public EventSerde(Class<T> contentType) {
		deserializer = new EventDeserializer<T>(contentType);
		
		Map<String, Object> serdeProps = new HashMap<>();
		
		serializer = new JsonPOJOSerializer<>();
	    serdeProps.put("JsonPOJOClass", Event.class);
	    serializer.configure(serdeProps, false);
	}
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Serializer<Event<T>> serializer() {
		// TODO Auto-generated method stub
		return serializer;
	}

	@Override
	public Deserializer<Event<T>> deserializer() {
		// TODO Auto-generated method stub
		return deserializer;
	}

}

class EventDeserializer<T> implements Deserializer<Event<T>>{
	private final ObjectMapper objectMapper = new ObjectMapper();
	private Class<T> contentType;
	
	public EventDeserializer (Class<T> contentType) {
		this.contentType = contentType;
	}
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Event<T> deserialize(String topic, byte[] data) {
		Event<T> obj = null;
        if (data == null)
            return null;

        try {
        	obj = objectMapper.readValue(data, objectMapper.getTypeFactory().constructParametricType(Event.class, contentType));
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return obj;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
