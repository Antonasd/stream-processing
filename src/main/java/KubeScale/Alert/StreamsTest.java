package KubeScale.Alert;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Enumeration;
import java.time.Duration;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;

import javafx.util.Pair;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import KubeScale.Alert.Serdes.StackSerde;


public class StreamsTest {
	
	final static int MAX_N_POINTS = 15;
	final static Double DELAY_THRESHOLD = 50.0;
	final static Double RATE_THRESHOLD = 5.0;
	final static Long SLA_THRESHOLD = 75L;
	
	final static Long ALERT_THRESHOLD = 50L;
	
	public final static Integer LEVEL_ERROR = 4;
	public final static Integer LEVEL_INFO = 1;
	public static void main(String[] args) {
		
	    Map<String, Object> serdeProps = new HashMap<>();

        final Deserializer<TwampData> twampDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampDeserializer.configure(serdeProps, false);
        
        final Serializer<TwampData> twampSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampSerializer.configure(serdeProps, false);
        
        final Serde<TwampData> twampSerde = Serdes.serdeFrom(twampSerializer, twampDeserializer);
        
        StackSerde<Double> serde = new StackSerde<Double>();
        
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test_stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "130.240.200.49:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, TwampData> source = builder.stream("twamp", Consumed.with(Serdes.String(), twampSerde));
		
		AverageDelayAlert avgDelayAlert = new AverageDelayAlert(50.0, 15, 8);
		avgDelayAlert.buildAverageDelayAlert(source);
		
		
		/*TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);
		
		final Serializer<TwampData> twampSerializerTest = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampDeserializer.configure(serdeProps, false);
        
		ConsumerRecordFactory<Long, TwampData> testFactory = new ConsumerRecordFactory<>("twamp", new LongSerializer(), twampSerializerTest);
		
		ArrayList<ConsumerRecord<byte[],byte[]>> testInput = new ArrayList<ConsumerRecord<byte[],byte[]>>();
		for(int i = 0; i<50; i++) {
			TwampData testData = new TwampData();
			testData.davg = 60.0;
			ConsumerRecord<byte[], byte[]> record = testFactory.create(1L, testData);
			testInput.add(record);
		}
		
		TwampData testData = new TwampData();
		testData.davg = 40.0;
		ConsumerRecord<byte[], byte[]> record = testFactory.create(1L, testData);
		testInput.add(record);
		
		testDriver.pipeInput(testInput);
		testDriver.close();*/
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp(); // dev only
        streams.start();
		
		
	}
	
	static void constructDelayAlert(KStream<Long, TwampData> source){
		Map<String, Object> serdeProps = new HashMap<>();

        final Deserializer<StateStatus> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", StateStatus.class);
        deserializer.configure(serdeProps, false);
        
        final Serializer<StateStatus> serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", StateStatus.class);
        serializer.configure(serdeProps, false);
        
        Serde<StateStatus> stateSerde = Serdes.serdeFrom(serializer, deserializer);
        
		KStream<Long, Stack<Double>> agg_stream = source
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
				Materialized.<Long, Stack<Double>, KeyValueStore<Bytes, byte[]>>as("last_n_stack")
					.withKeySerde(Serdes.Long())
					.withValueSerde(new StackSerde())
				)
			.toStream();
		
		KStream<Long, StateStatus>[] branches = agg_stream.mapValues(
					(value) -> {
						Enumeration<Double> enumerator = ((Stack<Double>)value).elements();
						int n_exceeding = 0;
						while(enumerator.hasMoreElements()){
							if(enumerator.nextElement() >= DELAY_THRESHOLD){
								n_exceeding++;
							}
						}
						return n_exceeding >= MAX_N_POINTS;
					})
			.groupByKey()
			.aggregate(
					() -> new StateStatus(),
					(key, value, agg) -> {
						if(agg.getStatus().equals(value)) {
							agg.setHasChanged(false);
						} else {
							System.out.println("I'm in 2");
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
	
	static void constructSLAAlert(KStream<Long, TwampData> source){
		KStream<Windowed<Long>, Long>[] alertBranches = source
				.mapValues((key, value) -> 100 - value.es)
				.filter((key, value) -> value <= StreamsTest.SLA_THRESHOLD)
				.groupByKey()
				.windowedBy(TimeWindows.of(Duration.ofMinutes(10L)).advanceBy(Duration.ofSeconds(150L)))
				.aggregate(
						() -> 0L,
						(key, value, agg) -> value >= StreamsTest.DELAY_THRESHOLD ? agg+1 : agg
						)
				.toStream()
				.branch(
						(key, value) -> value == 1L,
						(key, value) -> value == 1L,
						(key, value) -> value == 1L
						);
				
			alertBranches[0].print(Printed.toSysOut());
			alertBranches[1].print(Printed.toSysOut());
			alertBranches[2].print(Printed.toSysOut());
	}
	
	
	static void constructRateAlert(KStream<Long, TwampData> source) {
		KStream<Windowed<Long>, Long>[] alertBranches = source
				.mapValues((key, value) -> value.rate)
				.filter((key, value) -> value <= StreamsTest.RATE_THRESHOLD)
				.groupByKey()
				.windowedBy(TimeWindows.of(Duration.ofMinutes(10L)).advanceBy(Duration.ofSeconds(150L)))
				.aggregate(
						() -> 0L,
						(key, value, agg) -> value >= StreamsTest.DELAY_THRESHOLD ? agg+1 : agg
						)
				.toStream()
				.branch(
						(key, value) -> value == 1L,
						(key, value) -> value == 1L,
						(key, value) -> value == 1L
						);
				
			alertBranches[0].print(Printed.toSysOut());
			alertBranches[1].print(Printed.toSysOut());
			alertBranches[2].print(Printed.toSysOut());
	}
	
}

class TwampData {
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
	public Long dmin_far;
	public Long davg_far;
	public Long dmax_far;
	public Long dv_far;
	public Long loss_near;
	public Long miso_near;
	public Long dmin_near;
	public Long davg_near;
	public Long dmax_near;
	public Long dv_near;
	public Long es;
	public Long es_loss;
	public Long es_delay;
	public Long es_dv;
	public Long es_ses;
	public Long uas;
}

