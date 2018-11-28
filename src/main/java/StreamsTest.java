import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.time.Duration;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;


public class StreamsTest {
	
	final static Double DELAY_THRESHOLD = 50.0;
	final static Long ALERT_THRESHOLD = 15L;
	
	public static void main(String[] args) {
		
	    Map<String, Object> serdeProps = new HashMap<>();

        /*final Serializer<TwampData> twampSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampSerializer.configure(serdeProps, false);
        */
        final Deserializer<TwampData> twampDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", TwampData.class);
        twampDeserializer.configure(serdeProps, false);
        
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test_stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "druid-kafka:29092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, twampDeserializer);
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<Long, TwampData> source = builder.stream("twamp");
		KStream<Long, Double> source_reduced = source.mapValues((key, value) -> value.davg);
		
		source_reduced.groupByKey()
		  .windowedBy(TimeWindows.of(Duration.ofMinutes(5L)).advanceBy(Duration.ofMinutes(1L)))
		  .aggregate(
				  () -> 0L,
				  (key, value, agg) -> value >= StreamsTest.DELAY_THRESHOLD ? agg+1 : agg)
		  .toStream()
		  .print(Printed.toSysOut());
		
		  
		/*source_reduced.groupByKey()
			  .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)).advanceBy(TimeUnit.MINUTES.toMillis(1)))
			  .aggregate(
					  () -> 0L,
					  (key, value, agg) -> value >= StreamsTest.DELAY_THRESHOLD ? agg+1 : agg)
			  .toStream()
			  .filter((key, value) -> value == StreamsTest.ALERT_THRESHOLD)
			  .to("alert");*/
	}

}

final class TwampData {
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

