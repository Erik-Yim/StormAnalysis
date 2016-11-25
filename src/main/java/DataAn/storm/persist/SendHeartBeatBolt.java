package DataAn.storm.persist;

import java.io.Serializable;
import java.util.Date;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import DataAn.common.utils.DateUtil;
import DataAn.storm.Communication;
import DataAn.storm.hierarchy.BaseSimpleRichBolt;

@SuppressWarnings({"serial","unused"})
public class SendHeartBeatBolt extends BaseSimpleRichBolt {

	public SendHeartBeatBolt() {
		super(new Fields("time","communication"));
	}
	
	private _H util=new _H();
	
	@Override
	protected void doExecute(Tuple tuple) throws Exception {
		Communication communication= 
				(Communication) tuple.getValueByField("communication");
		util.heartbeat(communication);
	}
	
	private class _H implements Serializable{
		
		private long latestInTime;
		
		private void heartbeat(Communication communication){
			Date now=new Date();
			long nowTime=now.getTime();
			if(latestInTime==0){
				final String workflowDonePath="/flow/"+communication.getSequence()+"/persist/heartbeats";
				executor.setPath(workflowDonePath, DateUtil.format(now));
				latestInTime=nowTime;
			}else{
				if((nowTime-latestInTime)>30000){
					latestInTime=nowTime;
					final String workflowDonePath="/flow/"+communication.getSequence()+"/persist/heartbeats";
					executor.setPath(workflowDonePath, DateUtil.format(now));
				}
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
