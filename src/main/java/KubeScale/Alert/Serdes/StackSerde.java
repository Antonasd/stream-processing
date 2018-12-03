package KubeScale.Alert.Serdes;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import KubeScale.Alert.JsonPOJODeserializer;
import KubeScale.Alert.JsonPOJOSerializer;


public class StackSerde<T> implements Serde<Stack<T>> {
	
	JsonPOJOSerializer<Stack<T>> serializer;
	JsonPOJODeserializer<Stack<T>> deserializer;
	
	
	public StackSerde(){
		Map<String, Object> serdeProps = new HashMap<>();

		serializer = new JsonPOJOSerializer<Stack<T>>();
        serdeProps.put("JsonPOJOClass", Stack.class);
        serializer.configure(serdeProps, false);
        
        deserializer = new JsonPOJODeserializer<Stack<T>>();
        serdeProps.put("JsonPOJOClass", Stack.class);
        deserializer.configure(serdeProps, false);
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
	public Serializer<Stack<T>> serializer() {
		return serializer;
	}

	@Override
	public Deserializer<Stack<T>> deserializer() {
		return deserializer;
	}

}
