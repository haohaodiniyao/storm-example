package com.example.storm_example.word_count;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.example.storm_example.storm_kafka.MessageScheme;
import com.example.storm_example.word_count.bolts.WordCounter;
import com.example.storm_example.word_count.bolts.WordNormalizer;

public class TopologyMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("word-reader", new RandomSentenceSpout());
		BrokerHosts brokerHosts = new ZkHosts("192.168.157.128:2181,192.168.157.129:2181,192.168.157.130:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,"test-topic","","yaokai");
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		builder.setSpout("word-reader", new KafkaSpout(spoutConfig));
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(),4).fieldsGrouping("word-normalizer", new Fields("word"));
		
		Config conf = new Config();
		conf.put("wordsFile", "words.txt");
		conf.setDebug(false);
		
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());
        Thread.sleep(100*1000);
        
        //集群
//      StormSubmitter.submitTopology("word-count", conf, builder.createTopology());
        cluster.shutdown();
	}

}
