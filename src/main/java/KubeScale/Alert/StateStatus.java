package KubeScale.Alert;

public class StateStatus {

	public Boolean hasChanged;
	public Boolean status;
	
	public StateStatus(){
		hasChanged = false;
		status = false;
	}
	
	public void setHasChanged (Boolean hasChanged) {this.hasChanged = hasChanged;}
	public void setStatus (Boolean status) {this.status = status;}
	
	public Boolean changed() {return hasChanged;}
	public Boolean getStatus() {return status;}
	
	public String toString() {
		return "Status :"+status.toString() + ". Changed :"+hasChanged.toString();
	}
}