package KubeScale.Events;

public class Event<T> {
	public final static Integer LEVEL_INFO = 1;
	public final static Integer LEVEL_ERROR = 4;
	
	public String ts;
	public Integer level;
	public String cat;
	public String msg;
	public MetaData<T> data;
	
	public Event() {}
	
	public Event(String ts, Integer level, String cat, String msg, MetaData<T> data) {
		this.ts = ts;
		this.level = level;
		this.cat = cat;
		this.msg = msg;
		this.data = data;
	}
	
}
