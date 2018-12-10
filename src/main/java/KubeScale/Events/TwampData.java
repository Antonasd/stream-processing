package KubeScale.Events;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class TwampData {
	public Long timestamp;
	public Long account_id;
	public Long stream_id;
	public Long monitor_id;
	public Long task_id;
	public Long test_agent_sender_id;
	public Long reflector_id;
	public Double rate;
	public Double dmin;
	public Double davg;
	public Double dmax;
	public Double dv;
	public Long loss_far;
	public Long miso_far;
	public Double dmin_far;
	public Double davg_far;
	public Double dmax_far;
	public Double dv_far;
	public Long loss_near;
	public Long miso_near;
	public Double dmin_near;
	public Double davg_near;
	public Double dmax_near;
	public Double dv_near;
	public Long es;
	public Long es_loss;
	public Long es_delay;
	public Long es_dv;
	public Long es_ses;
	public Long uas;
	
	public static Serde<TwampData> getSerde(){
		Map<String, Object> serdeProps = new HashMap<>();

        final Deserializer<TwampData> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        deserializer.configure(serdeProps, false);
        
        final Serializer<TwampData> serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        serializer.configure(serdeProps, false);
        
        return Serdes.serdeFrom(serializer, deserializer);
	}
}
