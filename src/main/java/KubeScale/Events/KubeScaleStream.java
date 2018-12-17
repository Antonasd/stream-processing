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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


public class KubeScaleStream {
	public static void main(String[] args) {
		Namespace event_args;
		try {
			event_args = parseArgs(args);
		} catch (ArgumentParserException e) {
			System.err.println("Error parseing arguments.");
			e.printStackTrace();
			return;
		}
		
		Long sla_threshold = event_args.getLong("sla");
		Double davg_threshold = event_args.getDouble("davg");
		Double rate_threshold = event_args.getDouble("rate");
		Double davg_far_threshold = event_args.getDouble("davg_far");
		Long loss_far = event_args.getLong("loss_far");
		Long miso_far = event_args.getLong("miso_far");
		Double davg_near_threshold = event_args.getDouble("davg_near");
		Long loss_near = event_args.getLong("loss_near");
		Long miso_near = event_args.getLong("miso_near");
			
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event_threshold_stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "130.240.200.49:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, TwampData> source = builder.stream("twamp", Consumed.with(Serdes.String(), TwampData.getSerde()));
		
		/*EventBuilder<Long> slaEvent = new EventBuilder<Long>(source, sla_threshold, 
			(value) -> {
				int n = 0;
				Iterator<TwampData> iterator= value.iterator();
				while(iterator.hasNext()) {
					Long es = iterator.next().es;
					if(100L-es <= sla_threshold) n++;
				}
				return n >= 12;
			},
			"events");
		slaEvent.setCategory("THRESHOLD_ALERT");
		slaEvent.setThresholdExceedMessage("SLA is above "+sla_threshold+"%!");
		slaEvent.setBelowThreholdMessage("SLA is now below "+sla_threshold+"%.");
		slaEvent.build();*/
		
		EventBuilder<Double> delayEvent = new EventBuilder<Double>(source, davg_threshold, 
				(value) -> {
					Double aggregated_delay = 0.00;
					Iterator<TwampData> iterator = value.iterator();
					while(iterator.hasNext()) {
						aggregated_delay += iterator.next().davg;
					}
					return davg_threshold <= aggregated_delay/6;
				},
				"events");
		delayEvent.setCategory("THRESHOLD_ALERT");
		delayEvent.setThresholdExceedMessage("Average delay has exceeded "+davg_threshold+" ms!");
		delayEvent.setBelowThreholdMessage("Average delay is now below "+davg_threshold+" ms.");
		delayEvent.setNumberOfPoints(6);
		delayEvent.build();
		/*
		EventBuilder<Double> rateEvent = new EventBuilder<Double>(source, rate_threshold, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().rate <= rate_threshold) n++;
					}
					return n >= 12;
				},
				"events");
		rateEvent.setCategory("THRESHOLD_ALERT");
		rateEvent.setThresholdExceedMessage("Data rate is below "+rate_threshold+" mbps!");
		rateEvent.setBelowThreholdMessage("Data rate is now over "+rate_threshold+" mbps.");
		rateEvent.build();
		
		EventBuilder<Double> davgFarEvent = new EventBuilder<Double>(source, davg_far_threshold, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().davg_far >= davg_far_threshold) n++;
					}
					return n >= 12;
				},
				"events");
		
		davgFarEvent.setCategory("THRESHOLD_ALERT");
		davgFarEvent.setThresholdExceedMessage("Average delay at the far end has exceeded "+davg_far_threshold+" ms!");
		davgFarEvent.setBelowThreholdMessage("Average delay at the far end is now below "+davg_far_threshold+" ms.");
		davgFarEvent.build();
		
		EventBuilder<Long> lossFarEvent = new EventBuilder<Long>(source, loss_far, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= loss_far) n++;
					}
					return n >= 12;
				},
				"events");
		lossFarEvent.setCategory("THRESHOLD_ALERT");
		lossFarEvent.setThresholdExceedMessage("Packet loss at the far end has exceeded "+loss_far+"%!");
		lossFarEvent.setBelowThreholdMessage("Packet loss at the far end is now below "+loss_far+"%.");
		lossFarEvent.build();
		
		EventBuilder<Long> misoFarEvent = new EventBuilder<Long>(source, miso_far, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= miso_far) n++;
					}
					return n >= 12;
				},
				"events");
		misoFarEvent.setCategory("THRESHOLD_ALERT");
		misoFarEvent.setThresholdExceedMessage("The number of missordered packets at the far end is exceeding "+miso_far+".");
		misoFarEvent.setBelowThreholdMessage("The number of missordered packets at the far end is now below "+miso_far+".");
		misoFarEvent.build();
		
		EventBuilder<Double> davgNearEvent = new EventBuilder<Double>(source, davg_near_threshold, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().davg_near >= davg_near_threshold) n++;
					}
					return n >= 12;
				},
				"events");
		davgNearEvent.setCategory("THRESHOLD_ALERT");
		davgNearEvent.setThresholdExceedMessage("Average delay at the near end has exceeded "+davg_near_threshold+"!");
		davgNearEvent.setBelowThreholdMessage("Average delay at the near end is now below "+davg_near_threshold+".");
		davgNearEvent.build();
		
		EventBuilder<Long> lossNearEvent = new EventBuilder<Long>(source, loss_near, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= loss_near) n++;
					}
					return n >= 12;
				},
				"events");
		lossNearEvent.setCategory("THRESHOLD_ALERT");
		lossNearEvent.setThresholdExceedMessage("Packet loss at the near end has exceeded "+loss_near+"%!");
		lossNearEvent.setBelowThreholdMessage("Packet loss at the near end is now below "+loss_near+"%.");
		lossNearEvent.build();
		
		EventBuilder<Long> misoNearEvent = new EventBuilder<Long>(source, miso_near, 
				(value) -> {
					int n = 0;
					Iterator<TwampData> iterator= value.iterator();
					while(iterator.hasNext()) {
						if(iterator.next().loss_far >= miso_near) n++;
					}
					return n >= 12;
				},
				"events");
		misoNearEvent.setCategory("THRESHOLD_ALERT");
		misoNearEvent.setThresholdExceedMessage("The number of missordered packets at the near end is exceeding "+miso_near+".");
		misoNearEvent.setBelowThreholdMessage("The number of missordered packets at the near end is now below "+miso_near+".");
		misoNearEvent.build();
		*/
		//test(builder, props);
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        //streams.cleanUp(); // dev only
        streams.start();
		
	}
	
	public static Namespace parseArgs(String[] args) throws ArgumentParserException {
		
		ArgumentParser parser = ArgumentParsers.newFor("event_processor").build()
                .description("Processes events.");
		parser.addArgument("--sla")
			  .dest("sla")
			  .type(Long.class)
			  .setDefault(80L)
			  .help("Threshold for SLA (percentage).");
			
		parser.addArgument("--davg")
			  .dest("davg")
			  .type(Double.class)
			  .setDefault(150.0)
			  .help("Threshold for average delay.");
		
		parser.addArgument("--rate")
			  .dest("rate")
			  .type(Double.class)
			  .setDefault(50.0)
			  .help("Threshold for the data rate.");
		
		parser.addArgument("--davg_far")
			  .dest("davg_far")
			  .type(Double.class)
			  .setDefault(150.0)
			  .help("Threshold for average delay at the far end.");
		
		parser.addArgument("--loss_far")
			  .dest("loss_far")
			  .type(Long.class)
			  .setDefault(20L)
			  .help("Threshold for packet loss percentage at the far end.");
		
		parser.addArgument("--miso_far")
			  .dest("miso_far")
			  .type(Long.class)
			  .setDefault(200L)
			  .help("Threshold for the number of misordered packets at the far end.");
		
		parser.addArgument("--davg_near")
			  .dest("davg_near")
			  .type(Double.class)
			  .setDefault(150.0)
			  .help("Threshold for average delay at the near end.");
		
		parser.addArgument("--loss_near")
			  .dest("loss_near")
			  .type(Long.class)
			  .setDefault(20L)
			  .help("Threshold for packet loss percentage at the near end.");
	
		parser.addArgument("--miso_near")
			  .dest("miso_near")
			  .type(Long.class)
			  .setDefault(200L)
			  .help("Threshold for the number of misordered packets at the near end.");
		
		return parser.parseArgs(args);
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
