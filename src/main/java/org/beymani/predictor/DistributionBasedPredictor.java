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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.chombo.mr.HistogramField;
import org.chombo.mr.HistogramSchema;
import org.chombo.storm.Cache;
import org.chombo.storm.MessageQueue;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author pranab
 *
 */
public abstract class DistributionBasedPredictor extends ModelBasedPredictor {
	protected MessageQueue outQueue;
	protected Cache cache;
	protected Map<String, Integer> distrModel = new HashMap<String, Integer>();
	protected int totalCount;
	protected HistogramSchema schema;
	protected double scoreThreshold;
	protected StringBuilder stBld = new StringBuilder();
	protected String subFieldDelim = ";";

	public DistributionBasedPredictor(Map conf) {
		super();
		outQueue = MessageQueue.createMessageQueue(conf, conf.get("output.queue").toString());
		cache = Cache.createCache(conf);
		
		String modelKey =  conf.get("distribution.model.key").toString();
		String model = cache.get(modelKey);
		
		//distribution
		Scanner scanner = new Scanner(model);
		totalCount = 0;
		while (scanner.hasNextLine()) {
		  String line = scanner.nextLine();
		  String[] items = line.split(",");
		  int count = Integer.parseInt(items[1]);
		  totalCount += count;
		  distrModel.put(items[0], count);
		}
		
		//schema
		String schemaKey =  conf.get("schema.key").toString();
		String schemaStr = cache.get(schemaKey);
		ObjectMapper mapper = new ObjectMapper();
		try {
			schema = mapper.readValue(schemaStr, HistogramSchema.class);
		} catch (JsonParseException e) {
			throw new IllegalStateException("invalid JSON schema");
		} catch (JsonMappingException e) {
			throw new IllegalStateException("invalid JSON schema");
		} catch (IOException e) {
			throw new IllegalStateException("failed to parse JSON schema");
		}
		
		scoreThreshold =  Double.parseDouble(conf.get("score.threshold").toString());
		
	}
	
	/**
	 * @param record
	 * @return
	 */
	protected String getBucketKey(String record) {
		String[] items = record.split(",");
		
		stBld.delete(0, stBld.length());
		String bucketElement = null;
		for (HistogramField field : schema.getFields()) {
			String	item = items[field.getOrdinal()];
			if (field.isCategorical()){
				bucketElement = item;
			} else if (field.isInteger()) {
				bucketElement = "" + Integer.parseInt(item) / field.getBucketWidth();
			} else if (field.isDouble()) {
				bucketElement = "" + ((int)Double.parseDouble(item)) / field.getBucketWidth();
			}			
			stBld.append(bucketElement).append(subFieldDelim);
		}
		stBld.delete(stBld.length()-1, stBld.length());
		return stBld.toString();
	}
}
