package com.example.storm_example.word_count;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.example.storm_example.randomSentence.spout.RandomSentenceSpout;
import com.example.storm_example.word_count.bolts.WordCounter;
import com.example.storm_example.word_count.bolts.WordNormalizer;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new RandomSentenceSpout());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(),4).fieldsGrouping("word-normalizer", new Fields("word"));
		
		Config conf = new Config();
		conf.put("wordsFile", "words.txt");
		conf.setDebug(false);
		
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());
        Thread.sleep(10*1000);
        cluster.shutdown();
	}

}
