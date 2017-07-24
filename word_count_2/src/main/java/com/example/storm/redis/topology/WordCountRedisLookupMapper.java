package com.example.storm.redis.topology;

import java.util.List;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Lists;
/**
 * storm redis 查询
 * @author yaokai
 *
 */
public class WordCountRedisLookupMapper implements RedisLookupMapper {
        /**
	 * 
	 */
	private static final long serialVersionUID = 5335802431602984636L;
		private RedisDataTypeDescription description;
//        private final String hashKey = "wordCount";

        public WordCountRedisLookupMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.STRING);
        }

        @Override
        public List<Values> toTuple(ITuple input, Object value) {
            String member = getKeyFromTuple(input);
            List<Values> values = Lists.newArrayList();
            values.add(new Values(member, value));
            return values;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("wordName", "count"));
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