package com.example.storm_example.word_count.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.storm_example.randomSentence.spout.RandomSentenceSpout;

import redis.clients.jedis.Jedis;

public class WordCounter implements IRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(WordCounter.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = -4874570810524047268L;
	private Integer id;
	private String name;
	private Map<String,Integer> counters;
	private OutputCollector collector;
	private Jedis jedis;	
	@Override
	public void cleanup() {
		LOG.info("----------------单词统计----------------");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			jedis.set(entry.getKey(), String.valueOf(entry.getValue()));
			LOG.info(entry.getKey()+":"+entry.getValue());
		}
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		if(!counters.containsKey(word)){
			counters.put(word, 1);
		}else{
			Integer c = counters.get(word) + 1;
			counters.put(word, c);
		}
		collector.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.counters = new HashMap<String,Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		jedis = new Jedis("localhost");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
