package KubeScale.Alert;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.lang.System;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import KubeScale.Alert.Serdes.StackSerde;

public class AverageDelayAlert {
	private final Double AVG_DELAY_THRESHOLD;
	private final int MAX_N_POINTS;
	private final int ALERT_THRESHOLD;
	
	public AverageDelayAlert(Double avgDelayThreshold, int maxNpoints, int alertThreshold){
		AVG_DELAY_THRESHOLD = avgDelayThreshold;
		MAX_N_POINTS = maxNpoints;
		ALERT_THRESHOLD = alertThreshold;
	}
	public void buildAverageDelayAlert(KStream<String, TwampData> source) {
		Map<String, Object> serdePropsStatus = new HashMap<>();

        final Deserializer<StateStatus> deserializer = new JsonPOJODeserializer<>();
        serdePropsStatus.put("JsonPOJOClass", StateStatus.class);
        deserializer.configure(serdePropsStatus, false);
        
        final Serializer<StateStatus> serializer = new JsonPOJOSerializer<>();
        serdePropsStatus.put("JsonPOJOClass", StateStatus.class);
        serializer.configure(serdePropsStatus, false);
        
        Serde<StateStatus> stateSerde = Serdes.serdeFrom(serializer, deserializer);
        
        Map<String, Object> serdePropsEvent = new HashMap<>();
        
        final Deserializer<Event> deserializerEvent = new JsonPOJODeserializer<>();
        serdePropsEvent.put("JsonPOJOClass", Event.class);
        deserializerEvent.configure(serdePropsEvent, false);
        
        final Serializer<Event> serializerEvent = new JsonPOJOSerializer<>();
        serdePropsEvent.put("JsonPOJOClass", Event.class);
        serializerEvent.configure(serdePropsEvent, false);
        
        Serde<Event> eventSerde = Serdes.serdeFrom(serializerEvent, deserializerEvent);
        
        
        
		KStream<String, Stack<Double>> agg_stream = source
			.mapValues((key, value) -> value.davg)
			.groupByKey()
			.aggregate(
				() -> new Stack<Double>(),
				(key, value, agg) -> {
					if(agg.size() == MAX_N_POINTS){
						agg.pop();
						agg.push(value);
					} else {
						agg.push(value);
					}
					return agg;
				},
				Materialized.<String, Stack<Double>, KeyValueStore<Bytes, byte[]>>as("last_n_stack")
					.withKeySerde(Serdes.String())
					.withValueSerde(new StackSerde())
				)
			.toStream();
		
		KStream<String, StateStatus>[] branches = agg_stream.mapValues(
					(value) -> {
						Enumeration<Double> enumerator = ((Stack<Double>)value).elements();
						int n_exceeding = 0;
						while(enumerator.hasMoreElements()){
							if(enumerator.nextElement() >= AVG_DELAY_THRESHOLD){
								n_exceeding++;
							}
						}
						return n_exceeding >= MAX_N_POINTS;
					})
			.groupByKey()
			.aggregate(
					() -> new StateStatus(),
					(key, value, agg) -> {
						System.out.println(key);
						if(agg.getStatus().equals(value)) {
							agg.setHasChanged(false);
						} else {
							agg.setStatus(value);
							agg.setHasChanged(true);
						}
						return agg;
					},
					Materialized.<String, StateStatus, KeyValueStore<Bytes, byte[]>>as("state_status")
						.withKeySerde(Serdes.String())
						.withValueSerde(stateSerde)
					)
			.toStream()
			.branch(
					(key, value) -> value.changed() && value.getStatus(),
					(key, value) -> value.changed() && !value.getStatus());
		
		KStream<String, Event> error_stream = branches[0]
				.mapValues(
						(value) -> {
							System.out.println("Error level");
							return new Event(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX")
	                              .withZone(ZoneOffset.UTC)
	                              .format(Instant.now()),
	                              StreamsTest.LEVEL_ERROR,
	                              "derp",
	                              "Average delay has exceeded 50");
							});
		error_stream.to("events", Produced.with(Serdes.String(), eventSerde));
		
		KStream<String, Event> info_stream = branches[1]
				.mapValues(
						(value) -> {
							System.out.println("Error level");
							return new Event(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX")
	                        .withZone(ZoneOffset.UTC)
	                        .format(Instant.now()), 
	                        StreamsTest.LEVEL_INFO,
	                        "derp",
	                        "Average delay has returned to stable levels.");
							});
		
		info_stream.to("events", Produced.with(Serdes.String(), eventSerde));
		
		
	}
}
