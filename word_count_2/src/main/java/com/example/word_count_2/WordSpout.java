package com.example.word_count_2;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordSpout implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1555041263356598354L;
	boolean isDistributed;
	SpoutOutputCollector collector;
	public static final String[] words = new String[] { "apple", "orange", "pineapple", "banana", "watermelon" };

	public WordSpout() {
		this(true);
	}

	public WordSpout(boolean isDistributed) {
		this.isDistributed = isDistributed;
	}

	public boolean isDistributed() {
		return this.isDistributed;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void nextTuple() {
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		this.collector.emit(new Values(word),UUID.randomUUID());
		Thread.yield();
	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
