package com.example.storm_example.storm_kafka;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageScheme implements Scheme {
	  private static final Logger LOG = LoggerFactory.getLogger(MessageScheme.class);
	  private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
	@Override
	public List<Object> deserialize(ByteBuffer arg0) {
		try {
			String line = deserializeString(arg0);
			LOG.info("line--------------------------------------------"+line);
			return new Values(line);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	 public static String deserializeString(ByteBuffer string) {
	        if (string.hasArray()) {
	            int base = string.arrayOffset();
	            return new String(string.array(), base + string.position(), string.remaining());
	        } else {
	            return new String(Utils.toByteArray(string), UTF8_CHARSET);
	        }
	    }
	@Override
	public Fields getOutputFields() {
		return new Fields("line");
	}

}
