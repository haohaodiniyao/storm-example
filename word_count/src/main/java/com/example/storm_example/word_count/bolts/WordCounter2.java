package com.example.storm_example.word_count.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCounter2 extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(WordCounter2.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = -4874570810524047268L;
	private Integer id;
	private String name;
	private Map<String,Integer> counters;
	private OutputCollector collector;
	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input) {
		Object obj = input.getValue(0);
		String word = String.valueOf(obj);
		if(!counters.containsKey(word)){
			counters.put(word, 1);
		}else{
			Integer c = counters.get(word) + 1;
			counters.put(word, c);
		}
		if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && input.getSourceStreamId().equals(
			    Constants.SYSTEM_TICK_STREAM_ID)){
			List a = new ArrayList();
			a.add(input);
			collector.emit(a,new Values(counters.toString()));	
			counters.clear();
		}
		collector.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.counters = new ConcurrentHashMap<String,Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		 Map<String, Object> conf = new HashMap<String, Object>();
		    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);//每60s持久化一次数据
		    return conf;
	}

}
