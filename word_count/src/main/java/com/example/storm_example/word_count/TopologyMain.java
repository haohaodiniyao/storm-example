package com.example.storm_example.word_count;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.example.storm_example.storm_kafka.MessageScheme;
import com.example.storm_example.word_count.bolts.WordCounter2;
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
		builder.setBolt("word-counter", new WordCounter2(),4).fieldsGrouping("word-normalizer", new Fields("word"));
//		builder.setBolt("word-final", new WordFinal(),4).shuffleGrouping("word-counter");
		
		 //set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.157.128:9092,192.168.157.129:9092,192.168.157.130:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		 KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
	                .withProducerProperties(props)
	                .withTopicSelector(new DefaultTopicSelector("test-topic2"))
	                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("key","count"));

		
		builder.setBolt("word-final", bolt,4).shuffleGrouping("word-counter");
		
		Config conf = new Config();
		conf.put("wordsFile", "words.txt");
		conf.setDebug(false);
		conf.put("bootstrap.servers", "192.168.157.128:9092,192.168.157.129:9092,192.168.157.130:9092");
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());
        Thread.sleep(1000*1000);
        
        //集群
//      StormSubmitter.submitTopology("word-count", conf, builder.createTopology());
        cluster.shutdown();
	}

}
