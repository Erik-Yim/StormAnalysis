package DataAn.storm.exceptioncheck.model;

import java.util.List;
import java.util.Map;

public class TopJsondto{
	private String topname;
	private List<TopJsonparamdto>  jdparamlist;
	public String getTopname() {
		return topname;
	}
	public void setTopname(String topname) {
		this.topname = topname;
	}
	public List<TopJsonparamdto> getJdparamlist() {
		return jdparamlist;
	}
	public void setJdparamlist(List<TopJsonparamdto> jdparamlist) {
		this.jdparamlist = jdparamlist;
	}
	

}
