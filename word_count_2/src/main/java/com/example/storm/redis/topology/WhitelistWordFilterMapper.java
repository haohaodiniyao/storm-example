package com.example.storm.redis.topology;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

public class WhitelistWordFilterMapper implements RedisFilterMapper {
        /**
	 * 
	 */
	private static final long serialVersionUID = -4715206273398375897L;
		private RedisDataTypeDescription description;
        private final String setKey = "whitelist";

        public WhitelistWordFilterMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.SET, setKey);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return null;
        }
}