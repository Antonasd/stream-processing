package KubeScale.Alert;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

import KubeScale.Alert.Serdes.StackSerde;

public class SlaAlert {
	private final Long SLA_THRESHOLD;
	private final int MAX_N_POINTS;
	private final int ALERT_THRESHOLD;
	
	public SlaAlert(Long slaThreshold, int maxNpoints, int alertThreshold) {
		SLA_THRESHOLD = slaThreshold;
		MAX_N_POINTS = maxNpoints;
		ALERT_THRESHOLD = alertThreshold;
	}
	public void buildDelayAlert(KStream<Long, TwampData> source) {
		Map<String, Object> serdeProps = new HashMap<>();

        final Deserializer<StateStatus> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", StateStatus.class);
        deserializer.configure(serdeProps, false);
        
        final Serializer<StateStatus> serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", StateStatus.class);
        serializer.configure(serdeProps, false);
        
        Serde<StateStatus> stateSerde = Serdes.serdeFrom(serializer, deserializer);
        
		KStream<Long, Stack<Long>> agg_stream = source
			.mapValues((key, value) -> 100-value.es)
			.groupByKey()
			.aggregate(
				() -> new Stack<Long>(),
				(key, value, agg) -> {
					if(agg.size() == MAX_N_POINTS){
						agg.pop();
						agg.push(value);
					} else {
						agg.push(value);
					}
					return agg;
				},
				Materialized.<Long, Stack<Double>, KeyValueStore<Bytes, byte[]>>as("last_n_stack")
					.withKeySerde(Serdes.Long())
					.withValueSerde(new StackSerde())
				)
			.toStream();
		
		KStream<Long, StateStatus>[] branches = agg_stream.mapValues(
					(value) -> {
						Enumeration<Long> enumerator = value.elements();
						int n_exceeding = 0;
						while(enumerator.hasMoreElements()){
							if(enumerator.nextElement() >= SLA_THRESHOLD){
								n_exceeding++;
							}
						}
						return n_exceeding >= ALERT_THRESHOLD;
					})
			.groupByKey()
			.aggregate(
					() -> new StateStatus(),
					(key, value, agg) -> {
						if(agg.getStatus().equals(value)) {
							agg.setHasChanged(false);
						} else {
							agg.setStatus(value);
							agg.setHasChanged(true);
						}
						return agg;
					},
					Materialized.<Long, StateStatus, KeyValueStore<Bytes, byte[]>>as("state_status")
						.withKeySerde(Serdes.Long())
						.withValueSerde(stateSerde)
					)
			.toStream()
			.branch(
					(key, value) -> value.changed() && value.getStatus(),
					(key, value) -> value.changed() && !value.getStatus());
		
		branches[0].print(Printed.toSysOut());
		branches[1].print(Printed.toSysOut());
		
		
	}

}
