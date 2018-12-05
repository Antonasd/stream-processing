package KubeScale.Alert;

public class Event {
	public String ts;
	public Integer level;
	public String cat;
	public String msg;
	
	public Event() {}
	
	public Event(String ts, Integer level, String cat, String msg) {
		this.ts = ts;
		this.level = level;
		this.cat = cat;
		this.msg = msg;
	}
	
	
}
