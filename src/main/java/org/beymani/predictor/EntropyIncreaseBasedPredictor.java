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
public class EntropyIncreaseBasedPredictor extends ModelBasedPredictor {
	private MessageQueue outQueue;
	private Cache cache;
	private Map<String, Integer> distrModel = new HashMap<String, Integer>();
	private int totalCount;
	private double entropy;
	private double baseConvConst = Math.log(2);
	private HistogramSchema schema;
	private StringBuilder stBld = new StringBuilder();
	private String subFieldDelim = ":";
	
	public EntropyIncreaseBasedPredictor(Map conf) throws JsonParseException, JsonMappingException, IOException {
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
		
		//entropy
		entropy = 0;
		for (String bucketKey : distrModel.keySet()) {
			double pr = ((double)distrModel.get(bucketKey)) / totalCount;
			entropy += -pr * Math.log(pr) / baseConvConst;
		}
		
		//schema
		String schemaKey =  conf.get("schema.key").toString();
		String schemaStr = cache.get(schemaKey);
		ObjectMapper mapper = new ObjectMapper();
		schema = mapper.readValue(schemaStr, HistogramSchema.class);
	}

	@Override
	public double execute(String entityID, String record) {
		double score = 0;
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
		String thisBucketKey = stBld.toString();
		
		//new entropy
		double newEntropy = 0;
		int newTotalCount = totalCount + 1;
		boolean bucketFound = false;
		double pr = 0;
		for (String bucketKey : distrModel.keySet()) {
			if (bucketKey.equals(thisBucketKey)) {
				pr = ((double)distrModel.get(bucketKey) + 1) / newTotalCount;
				bucketFound = true;
			} else {
				pr = ((double)distrModel.get(bucketKey)) / newTotalCount;
			}
			newEntropy += -pr * Math.log(pr) / baseConvConst;
		}
		
		if (!bucketFound) {
			pr = 1.0 / newTotalCount;
			newEntropy += -pr * Math.log(pr) / baseConvConst;
		}
		
		if (newEntropy > entropy) {
			score = (newEntropy - entropy) / entropy;
		}
		return score;
	}

}
