package com.example.storm_example.word_count.bolts;

import java.text.SimpleDateFormat;
import java.util.Date;
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

public class WordFinal implements IRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(WordFinal.class);
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

	}

	@Override
	public void execute(Tuple input) {
		String sc = input.getSourceComponent();
		String count = input.getString(0);
		LOG.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date())+"count->"+sc+"->"+count);
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
