package KubeScale.Events.Serdes;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import KubeScale.Events.JsonPOJOSerializer;

public class LinkedListSerde<T> implements Serde<LinkedList<T>> {
	
	Serializer<LinkedList<T>> serializer;
	Deserializer<LinkedList<T>> deserializer;
	
	
	public LinkedListSerde(Class<T> contentType){
		deserializer = new LinkedListDeserializer<T>(contentType);
		
		Map<String, Object> serdeProps = new HashMap<>();
		
		serializer = new JsonPOJOSerializer<>();
	    serdeProps.put("JsonPOJOClass", LinkedList.class);
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
	public Serializer<LinkedList<T>> serializer() {
		return serializer;
	}

	@Override
	public Deserializer<LinkedList<T>> deserializer() {
		return deserializer;
	}

}

class LinkedListDeserializer<T> implements Deserializer<LinkedList<T>>{
	private final ObjectMapper objectMapper = new ObjectMapper();
	private Class<T> contentType;
	
	public LinkedListDeserializer (Class<T> contentType){
		this.contentType = contentType;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public LinkedList<T> deserialize(String topic, byte[] data) {
		LinkedList<T> obj = null;
        if (data == null)
            return null;

        try {
        	obj = objectMapper.readValue(data, objectMapper.getTypeFactory().constructParametricType(LinkedList.class, contentType));
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
