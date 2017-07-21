package com.example.word_count_2;

import java.util.Map;

import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCounter implements IBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 153254012544891054L;
	private Map<String,Integer> wordCounter = Maps.newConcurrentMap();
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getStringByField("word");
		int count;
		if(wordCounter.containsKey(word)){
			count = wordCounter.get(word) + 1;
			wordCounter.put(word, wordCounter.get(word) + 1);
		}else{
			count = 1;
			wordCounter.put(word,1);
		}
		collector.emit(new Values(word,String.valueOf(count)));
	}

	@Override
	public void cleanup() {
		
	}

}
