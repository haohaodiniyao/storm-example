package com.example.storm_example.word_count.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordReader implements IRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;
	@Override
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}

	@Override
	public void activate() {

	}

	@Override
	public void close() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL"+msgId);
	}

	@Override
	public void nextTuple() {
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}
		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while((str=reader.readLine())!=null){
				this.collector.emit(new Values(str),str);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed = true;
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]",e);
		}
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
