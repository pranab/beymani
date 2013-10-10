/*
 * beymani: Outlier and anamoly detection 
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.beymani.predictor;

import java.util.Map;

import redis.clients.jedis.Jedis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author pranab
 *
 */
public class RedisSpout  extends  BaseRichSpout {
    private SpoutOutputCollector collector;
    private Map conf;
	private Jedis jedis;
	private String messageQueue;
	private static final String NIL = "nil";

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
		String redisHost = conf.get("redis.server.host").toString();
		int redisPort = new Integer(conf.get("redis.server.port").toString());
		jedis = new Jedis(redisHost, redisPort);
		messageQueue =  conf.get("redis.message.queue").toString();
	}

	@Override
	public void nextTuple() {
		String message  = jedis.rpop(messageQueue);		
		if(null != message  && !message.equals(NIL)) {
			int pos = message.indexOf(",");
			String entityID = message.substring(0, pos);
			String recordData = message.substring(pos+1);
			collector.emit(new Values(entityID, recordData));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entityID", "recordData"));		
	}

}
