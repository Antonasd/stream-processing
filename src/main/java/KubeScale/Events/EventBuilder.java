package KubeScale.Events;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

import KubeScale.Events.Serdes.EventSerde;
import KubeScale.Events.Serdes.LinkedListSerde;
import KubeScale.Events.Serdes.MetaDataSerde;

public class EventBuilder<T> {
	private T threshold;
	private Class<T> thresholdType;
	private ValueMapper<LinkedList<TwampData>, Boolean> valueMapper;
	
	private KStream<String, TwampData> source;
	private String topic;
	
	private int nPoints = 15;
	
	private String thresholdExceededMessage = "Threshold exceeded";
	private String belowThresholdMessage = "Threshold exceeded";
	private String category = "Test";
	
	public EventBuilder(
			KStream<String, TwampData> source, 
			T threshold,  
			ValueMapper<LinkedList<TwampData>, Boolean> valueMapper,
			String topic) 
	{
		this.source = source;
		this.threshold = threshold;
		this.valueMapper = valueMapper;
		this.topic = topic;
		this.thresholdType = (Class<T>) threshold.getClass();
	}
	
	public void setNumberOfPoints(int nPoints) {
		this.nPoints = nPoints;
	}
	
	public void setThresholdExceedMessage(String thresholdExceededMessage) {
		this.thresholdExceededMessage = thresholdExceededMessage;
	}
	
	public void setBelowThreholdMessage(String belowThresholdMessage) {
		this.belowThresholdMessage = belowThresholdMessage;
	}
	
	public void setCategory(String category) {
		this.category = category;
	}
	
	public void build() {
		//Sets key to meta data
		KStream<MetaData<T>, TwampData> reMappedStream = source
				.map((streamId, record) ->{
						MetaData<T> metaData = new MetaData<T>(streamId, record.task_id, record.monitor_id, threshold);
						return KeyValue.pair(metaData, record);
					});
		
		//Calculates if the threshold has been exceeded based on the last n records.
		KStream<MetaData<T>, Boolean> aggregatedStream = reMappedStream
			.groupByKey(Grouped.with(new MetaDataSerde<T>(thresholdType), TwampData.getSerde()))
			.aggregate(
				() -> new LinkedList<TwampData>(),
				(metaData, value, stack) -> {
					if(stack.size() == nPoints){
						stack.pollLast();
						stack.push(value);
					} else {
						stack.push(value);
					}
					return stack;
				},
				Materialized.<MetaData<T>, LinkedList<TwampData>, KeyValueStore<Bytes, byte[]>>
					with(new MetaDataSerde<T>(thresholdType), new LinkedListSerde<TwampData>(TwampData.class))
				)
			.toStream()
			.mapValues(valueMapper);
		
		//Updates the state
		KStream<MetaData<T>, State> stateStream = aggregatedStream
				.groupByKey(Grouped.<MetaData<T>>keySerde(new MetaDataSerde<T>(thresholdType)))
				.aggregate(
						new Initializer<State>() {
						      @Override
						      public State apply() {
						        return new State();
						      }
						},
						new Aggregator<MetaData<T>, Boolean, State>() {
							@Override
							public State apply(MetaData<T> metaData, Boolean tresholdExceeded, State state){
								if(state.thresholdExceeded.equals(tresholdExceeded)) {
									state.hasChanged = false;
								} else {
									System.out.println("Event triggered for stream: "+metaData.stream_id);
									state.thresholdExceeded = tresholdExceeded;
									state.hasChanged = true;
								}
								return state;
							}
						},
						Materialized.<MetaData<T>, State, KeyValueStore<Bytes, byte[]>>
							with(new MetaDataSerde<T>(thresholdType), State.getSerde())
						)
				.toStream();
		
		//Sends an event if the state has changed.
		@SuppressWarnings("unchecked")
		KStream<MetaData<T>, State>[] eventBranches = stateStream
				.branch(
						(streamId, newState) -> newState.hasChanged && newState.thresholdExceeded,
						(streamId, newState) -> newState.hasChanged 
						);
		
		//Event error.
		eventBranches[0]
				.map((metaData, state) -> {
					System.out.println("Constructing error event with id: "+metaData.stream_id);
					return new KeyValue<String, Event<T>>(metaData.stream_id, 
					new Event<T>(
						DateTimeFormatter
							.ofPattern("yyyy-MM-dd'T'HH:mmX")
			                .withZone(ZoneOffset.UTC)
			                .format(Instant.now()),
		                Event.LEVEL_ERROR,
		                category,
		                thresholdExceededMessage,
		                metaData
						)
					);})
				.to(topic, Produced.<String, Event<T>>with(Serdes.String(), new EventSerde<T>(thresholdType)));
		
		//Event info (There is no longer an error).
		eventBranches[1]
				.map((metaData, state) ->{
					System.out.println("Constructing info event with id: "+metaData.stream_id);
					return new KeyValue<String, Event<T>>(metaData.stream_id, 
					new Event<T>(
						DateTimeFormatter
							.ofPattern("yyyy-MM-dd'T'HH:mmX")
			                .withZone(ZoneOffset.UTC)
			                .format(Instant.now()),
			            Event.LEVEL_INFO,
			            category,
			            belowThresholdMessage,
			            metaData
						)
					);})
				.to(topic, Produced.<String, Event<T>>with(Serdes.String(), new EventSerde<T>(thresholdType)));
	}
	
}
