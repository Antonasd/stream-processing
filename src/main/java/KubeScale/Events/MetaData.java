package KubeScale.Events;


public class MetaData<T> {
	public String stream_id;
	public Long monitor_id;
	public Long task_id;
	public T threshold;
	
	public MetaData() {
		
	}
	public MetaData(String stream_id, Long task_id, Long monitor_id, T treshold){
		this.stream_id = stream_id;
		this.task_id = task_id;
		this.monitor_id = monitor_id;	
		this.threshold = treshold;
	}
}