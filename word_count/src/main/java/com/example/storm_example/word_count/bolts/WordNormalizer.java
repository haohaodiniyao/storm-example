package com.example.storm_example.word_count.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordNormalizer implements IRichBolt {
	private OutputCollector collector;
	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getStringByField("line");
		String[] words = sentence.split(" ");
		for(String word:words){
			word = word.trim();
			if(!word.isEmpty()){
				word = word.toLowerCase();
				List a = new ArrayList();
				a.add(input);
				collector.emit(a,new Values(word));
			}
		}
		collector.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
