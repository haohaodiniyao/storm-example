package com.example.storm.redis.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String STORE_BOLT = "STORE_BOLT";

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
        WordCounter bolt = new WordCounter();
        RedisStoreMapper storeMapper = setupStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

        // wordSpout ==> countBolt ==> RedisBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).fieldsGrouping(WORD_SPOUT, new Fields("word"));
        builder.setBolt(STORE_BOLT, storeBolt, 1).shuffleGrouping(COUNT_BOLT);

        String topoName = "test_set_redis";
        if (args.length == 3) {
            topoName = args[2];
        } else if (args.length > 3) {
            System.out.println("Usage: PersistentWordCount <redis host> <redis port> (topology name)");
            return;
        }
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.createTopology());
//        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
    }

    private static RedisStoreMapper setupStoreMapper() {
        return new WordCountStoreMapper();
    }
}