package KubeScale.Events;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class TwampData {
	public Long timestamp = 0L;
	public Long account_id = 0L;
	public Long stream_id = 0L;
	public Long monitor_id = 0L;
	public Long task_id = 0L;
	public Long test_agent_sender_id = 0L;
	public Long reflector_id = 0L;
	public Double rate = 0.0;
	public Double dmin = 0.0;
	public Double davg = 0.0;
	public Double dmax = 0.0;
	public Double dv = 0.0;
	public Long loss_far = 0L;
	public Long miso_far = 0L;
	public Double dmin_far = 0.0;
	public Double davg_far = 0.0;
	public Double dmax_far = 0.0;
	public Double dv_far = 0.0;
	public Long loss_near = 0L;
	public Long miso_near = 0L;
	public Double dmin_near = 0.0;
	public Double davg_near = 0.0;
	public Double dmax_near = 0.0;
	public Double dv_near = 0.0;
	public Long es = 0L;
	public Long es_loss = 0L;
	public Long es_delay = 0L;
	public Long es_dv = 0L;
	public Long es_ses = 0L;
	public Long uas = 0L;
	
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
