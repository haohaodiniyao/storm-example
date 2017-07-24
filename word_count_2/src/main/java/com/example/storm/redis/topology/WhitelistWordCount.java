package com.example.storm.redis.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisFilterBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WhitelistWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String WHITELIST_BOLT = "WHITELIST_BOLT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    private static final String TEST_REDIS_HOST = "127.0.0.1";
    private static final int TEST_REDIS_PORT = 6379;

    public static void main(String[] args) throws Exception {
        Config config = new Config();

        String host = TEST_REDIS_HOST;
        int port = TEST_REDIS_PORT;

        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();

        WordSpout spout = new WordSpout();
        RedisFilterMapper filterMapper = setupWhitelistMapper();
        RedisFilterBolt whitelistBolt = new RedisFilterBolt(poolConfig, filterMapper);
        WordCounter wordCounterBolt = new WordCounter();
        PrintWordTotalCountBolt printBolt = new PrintWordTotalCountBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(WHITELIST_BOLT, whitelistBolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(COUNT_BOLT, wordCounterBolt, 1).fieldsGrouping(WHITELIST_BOLT, new Fields("word"));
        builder.setBolt(PRINT_BOLT, printBolt, 1).shuffleGrouping(COUNT_BOLT);

        String topoName = "test-filter-redis";
        if (args.length == 3) {
            topoName = args[2];
        } else if (args.length > 3) {
            System.out.println("Usage: WhitelistWordCount <redis host> <redis port> [topology name]");
            return;
        }
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.createTopology());
        Thread.sleep(100*1000);
        cluster.shutdown();
//        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
    }

    private static RedisFilterMapper setupWhitelistMapper() {
        return new WhitelistWordFilterMapper();
    }
}