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

import org.apache.hadoop.conf.Configuration;
import org.chombo.util.RichAttribute;

/**
 * Outlier detection based weighted cumulative probability of all attributes
 * @author pranab
 *
 */
public class EsimatedAttrtibuteProbabilityBasedPredictor extends DistributionBasedPredictor {
	private Map<Integer, Map<String, Integer>> attrDistr = new HashMap<Integer, Map<String, Integer>>();
	private Map<Integer, Integer> attrDistrCounts = new HashMap<Integer, Integer>();
	private double[] attrWeights;
	private boolean requireMissingAttrValue;
	private boolean realTimeDetection;
	
	/**
	 * Storm usage
	 * @param conf
	 */
	public EsimatedAttrtibuteProbabilityBasedPredictor(Map conf) {
		super(conf);
		
		//per attribute distribution
		buildAttributeWiseDistr();
		
		//attribute weights
		String[] weightStrs =  conf.get("attr.weight").toString().split(",");
		attrWeights = new double[weightStrs.length];
		for (int a = 0; a < weightStrs.length; ++a) {
			attrWeights[a] = Double.parseDouble(weightStrs[a]);
		}
		
		requireMissingAttrValue = Boolean.parseBoolean(conf.get("require.missing.attr.value").toString());
		realTimeDetection = true;
	}

	/**
	 * @param config
	 * @param distrFilePath
	 * @throws IOException
	 */
	public EsimatedAttrtibuteProbabilityBasedPredictor(Configuration config, String distrFilePath) throws IOException {
		super(config, distrFilePath);
		
		buildAttributeWiseDistr();

		//attribute weights
		String[] weightStrs =  config.get("attr.weight").split(",");
		attrWeights = new double[weightStrs.length];
		for (int a = 0; a < weightStrs.length; ++a) {
			attrWeights[a] = Double.parseDouble(weightStrs[a]);
		}
	}
	
	/**
	 * 
	 */
	private void buildAttributeWiseDistr() {
		//per attribute distribution
		int i = 0;
		for (RichAttribute field : schema.getFields()) {
			Integer ordinal = field.getOrdinal();
			Map<String, Integer> distr = attrDistr.get(ordinal);
			if (null == distr){
				distr = new HashMap<String, Integer>();
				attrDistr.put(ordinal, distr);
			}
			int totalCount = 0;
			for (String bucket : distrModel.keySet()) {
				String[] items = bucket.split(subFieldDelim);
				String attrBucket = items[i];
				int bucketCount = distrModel.get(bucket);
				Integer count = distr.get(attrBucket);
				if (null == count) {
					distr.put(attrBucket, bucketCount);
				} else {
					distr.put(attrBucket, count + bucketCount);
				}
				totalCount += bucketCount;
			}
			attrDistrCounts.put(ordinal, totalCount);
			++i;
		}
	}
	
	
	@Override
	public double execute(String entityID, String record) {
		String bucketKey = getBucketKey(record);
		String[] bucketElements = bucketKey.split(subFieldDelim);
		int i = 0;
		double score = 0;
		int rareCount = 0;
		for (RichAttribute field : schema.getFields()) {
			Integer ordinal = field.getOrdinal();
			String bucketElem = bucketElements[i];
			Integer count  = attrDistr.get(ordinal).get(bucketElem);
			if (null == count){
				++rareCount;
			}
			double pr = count != null ? ((double)count / attrDistrCounts.get(ordinal)) : 0;
			score += attrWeights[i] * (1.0 - pr);
			++i;
		}
		
		if (requireMissingAttrValue && rareCount == 0) {
			score = 0;
		}
		if (realTimeDetection && score > scoreThreshold) {
			//write if above threshold
			outQueue.send(entityID + " " + score);
		}

		return score;
	}

	
}
