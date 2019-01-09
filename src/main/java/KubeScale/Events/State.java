package KubeScale.Events;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class State {
	public Boolean hasChanged = false;
	public Boolean thresholdExceeded;
	
	public State(){
		thresholdExceeded = false;
	}
	
	public static Serde<State> getSerde(){
		Map<String, Object> serdeProps = new HashMap<>();

        final Deserializer<State> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", State.class);
        deserializer.configure(serdeProps, false);
        
        final Serializer<State> serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", State.class);
        serializer.configure(serdeProps, false);
        
        return Serdes.serdeFrom(serializer, deserializer);
	}
}

