package KubeScale.Events;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.time.Duration;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.test.ConsumerRecordFactory;



public class StreamsTest {
	
	final static int MAX_N_POINTS = 15;
	final static Double DELAY_THRESHOLD = 50.0;
	final static Double RATE_THRESHOLD = 5.0;
	final static Long SLA_THRESHOLD = 75L;
	
	final static Long ALERT_THRESHOLD = 50L;
	
	public final static Integer LEVEL_ERROR = 4;
	public final static Integer LEVEL_INFO = 1;
	public static void hey(String[] args) {
		
	    Map<String, Object> serdeProps = new HashMap<>();

        final Deserializer<TwampData> twampDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampDeserializer.configure(serdeProps, false);
        
        final Serializer<TwampData> twampSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampSerializer.configure(serdeProps, false);
        

        final Serde<TwampData> twampSerde = Serdes.serdeFrom(twampSerializer, twampDeserializer);
        
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test_stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "130.240.200.49:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, TwampData> source = builder.stream("twamp", Consumed.with(Serdes.String(), twampSerde));
		
		
		EventBuilder<Long> slaEvent = new EventBuilder<Long>(source, new Long(70), (value) -> {
				int n = 0;
				Iterator<TwampData> iterator= value.iterator();
				while(iterator.hasNext()) {
					if(100L-iterator.next().es <= 95L) n++;
				}
				return n >= 12;
			},
				"events");
		
		slaEvent.build();
		/*
		TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);
		
		final Serializer<TwampData> twampSerializerTest = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampDeserializer.configure(serdeProps, false);
        
		ConsumerRecordFactory<String, TwampData> testFactory = new ConsumerRecordFactory<>("twamp", new StringSerializer(), twampSerializerTest);
		
		ArrayList<ConsumerRecord<byte[],byte[]>> testInput = new ArrayList<ConsumerRecord<byte[],byte[]>>();
		for(int i = 0; i<50; i++) {
			TwampData testData = new TwampData();
			testData.es = 60L;
			ConsumerRecord<byte[], byte[]> record = testFactory.create(testData);
			testInput.add(record);
		}
		
		for(int i = 0; i<50; i++) {
			TwampData testData = new TwampData();
			testData.es = 20L;
			ConsumerRecord<byte[], byte[]> record = testFactory.create(testData);
			testInput.add(record);
		}
		
		
		testDriver.pipeInput(testInput);
		testDriver.close();
		*/
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp(); // dev only
        streams.start();
        
        
		
	}

	
}



