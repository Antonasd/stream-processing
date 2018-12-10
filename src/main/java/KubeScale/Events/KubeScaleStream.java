package KubeScale.Events;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.ConsumerRecordFactory;


public class KubeScaleStream {
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test_stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "130.240.200.49:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, TwampData> source = builder.stream("twamp", Consumed.with(Serdes.String(), TwampData.getSerde()));
		
		EventBuilder<Long> slaEvent = new EventBuilder<Long>(source, new Long(70), 
			(value) -> {
				int n = 0;
				Iterator<TwampData> iterator= value.iterator();
				while(iterator.hasNext()) {
					Long es = iterator.next().es;
					System.out.println(es);
					if(100L-es <= 100L) n++;
				}
				return n >= 12;
			},
			"events");
		slaEvent.setCategory("sla");
		slaEvent.setThresholdExceedMessage("SLA threshold exceeded!");
		slaEvent.setBelowThreholdMessage("SLA is now below 95%");
		slaEvent.build();
		
		EventBuilder<Double> delayEvent = new EventBuilder<Double>(source, 50.0, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().davg >= 50.0) n++;
					}
					return n >= 12;
				},
				"events");
		delayEvent.setCategory("average_delay");
		delayEvent.setThresholdExceedMessage("Delay threshold exceeded!");
		delayEvent.setBelowThreholdMessage("Average delay is now below 50.0 ms.");
		delayEvent.build();
		
		EventBuilder<Double> rateEvent = new EventBuilder<Double>(source, 60.0, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().rate <= 50.0) n++;
					}
					return n >= 12;
				},
				"events");
		rateEvent.setCategory("rate");
		rateEvent.setThresholdExceedMessage("Data rate below threshold!");
		rateEvent.setBelowThreholdMessage("Data rate is now over 60 mbps");
		rateEvent.build();
		
		EventBuilder<Double> davgFarEvent = new EventBuilder<Double>(source, 50.0, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().davg_far >= 50.0) n++;
					}
					return n >= 12;
				},
				"events");
		davgFarEvent.setCategory("average_delay_far");
		davgFarEvent.setThresholdExceedMessage("Far delay threshold exceeded!");
		davgFarEvent.setBelowThreholdMessage("Average far delay is now below 50.0 ms.");
		davgFarEvent.build();
		
		EventBuilder<Long> lossFarEvent = new EventBuilder<Long>(source, 60L, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= 60L) n++;
					}
					return n >= 12;
				},
				"events");
		lossFarEvent.setCategory("loss_far");
		lossFarEvent.setThresholdExceedMessage("Packet loss at the far end has exceeded the threshold!");
		lossFarEvent.setBelowThreholdMessage("Packet loss at the far end is now below the threshold.");
		lossFarEvent.build();
		
		EventBuilder<Long> misoFarEvent = new EventBuilder<Long>(source, 20L, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= 20L) n++;
					}
					return n >= 12;
				},
				"events");
		misoFarEvent.setCategory("miso_far");
		misoFarEvent.setThresholdExceedMessage("The number of missordered packets at the far end is exceeding the threshold.");
		misoFarEvent.setBelowThreholdMessage("The number of missordered packets at the far end is now below the threshold");
		misoFarEvent.build();
		
		EventBuilder<Double> davgNearEvent = new EventBuilder<Double>(source, 50.0, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().davg_near >= 50.0) n++;
					}
					return n >= 12;
				},
				"events");
		davgNearEvent.setCategory("average_delay_near");
		davgNearEvent.setThresholdExceedMessage("Near delay threshold exceeded!");
		davgNearEvent.setBelowThreholdMessage("Average near delay is now below 50.0 ms.");
		davgNearEvent.build();
		
		EventBuilder<Long> lossNearEvent = new EventBuilder<Long>(source, 60L, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= 60L) n++;
					}
					return n >= 12;
				},
				"events");
		lossNearEvent.setCategory("loss_near");
		lossNearEvent.setThresholdExceedMessage("Packet loss at the near end has exceeded the threshold!");
		lossNearEvent.setBelowThreholdMessage("Packet loss at the near end is now below the threshold.");
		lossNearEvent.build();
		
		EventBuilder<Long> misoNearEvent = new EventBuilder<Long>(source, 20L, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= 20L) n++;
					}
					return n >= 12;
				},
				"events");
		misoNearEvent.setCategory("miso_near");
		misoNearEvent.setThresholdExceedMessage("The number of missordered packets at the near end is exceeding the threshold.");
		misoNearEvent.setBelowThreholdMessage("The number of missordered packets at the near end is now below the threshold");
		misoNearEvent.build();
		
		//test(builder, props);
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp(); // dev only
        streams.start();
		
	}
	
	public static void test(StreamsBuilder builder, Properties props) {
		Map<String, Object> serdeProps = new HashMap<>();
		TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);
		
		final Serializer<TwampData> twampSerializerTest = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampSerializerTest.configure(serdeProps, false);
        
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
	}
}
