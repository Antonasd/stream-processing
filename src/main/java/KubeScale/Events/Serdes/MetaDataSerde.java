package KubeScale.Events.Serdes;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import KubeScale.Events.JsonPOJOSerializer;
import KubeScale.Events.MetaData;

public class MetaDataSerde<T>  implements Serde<MetaData<T>>{
	
	Serializer<MetaData<T>> serializer;
	Deserializer<MetaData<T>> deserializer;
	
	public MetaDataSerde(Class<T> contentType) {
		deserializer = new MetaDataDeserializer<T>(contentType);
		
		Map<String, Object> serdeProps = new HashMap<>();
		
		serializer = new JsonPOJOSerializer<>();
	    serdeProps.put("JsonPOJOClass", MetaData.class);
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
	public Serializer<MetaData<T>> serializer() {
		return serializer;
	}

	@Override
	public Deserializer<MetaData<T>> deserializer() {
		return deserializer;
	}

}

class MetaDataDeserializer<T> implements Deserializer<MetaData<T>> {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private Class<T> contentType;
	 
	public MetaDataDeserializer (Class<T> contentType) {
		this.contentType = contentType;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MetaData<T> deserialize(String topic, byte[] data) {
		MetaData<T> obj = null;
        if (data == null)
            return null;

        try {
        	obj = objectMapper.readValue(data, objectMapper.getTypeFactory().constructParametricType(MetaData.class, contentType));
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