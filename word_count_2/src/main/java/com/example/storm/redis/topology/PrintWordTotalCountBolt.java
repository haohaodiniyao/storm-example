package com.example.storm.redis.topology;

import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintWordTotalCountBolt extends BaseRichBolt {
        /**
	 * 
	 */
	private static final long serialVersionUID = -2178624600049446261L;
		private static final Logger LOG = LoggerFactory.getLogger(PrintWordTotalCountBolt.class);
        private static final Random RANDOM = new Random();
        private OutputCollector collector;

        @SuppressWarnings("rawtypes")
		@Override
        public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String wordName = input.getStringByField("word");
            String countStr = input.getStringByField("count");

            // print lookup result with low probability
            if(RANDOM.nextInt(1000) > 995) {
                int count = 0;
                if (countStr != null) {
                    count = Integer.parseInt(countStr);
                }
                LOG.info("Lookup result - word : " + wordName + " / count : " + count);
            }

            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
}