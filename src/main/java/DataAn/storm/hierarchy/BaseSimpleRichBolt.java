package DataAn.storm.hierarchy;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings({"serial","rawtypes"})
public abstract class BaseSimpleRichBolt extends org.apache.storm.topology.base.BaseRichBolt {

	private Map stormConf;
	
	private TopologyContext context;
	
	private OutputCollector collector;
	
	private Fields fields; 
	
	public BaseSimpleRichBolt(Fields fields) {
		this.fields = fields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		this.stormConf=stormConf;
		this.context=context;
	}
	
	protected final Map getStormConf() {
		return stormConf;
	}
	
	@Override
	public final void execute(Tuple tuple) {
		try{
			doExecute(tuple);
		}catch (Exception e) {
			throw new FailedException(e);
		}
	}
	
	protected void emit(Values values){
		collector.emit(values);
	}
	
	
	protected abstract void doExecute(Tuple tuple) throws Exception;
	

	@Override
	public final void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}
}
