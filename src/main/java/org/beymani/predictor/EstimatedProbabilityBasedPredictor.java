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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Estimated probability based outlier prediction
 * @author pranab
 *
 */
public class EstimatedProbabilityBasedPredictor extends DistributionBasedPredictor {

	/**
	 * Storm usage
	 * @param conf
	 */
	public EstimatedProbabilityBasedPredictor(Map conf) {
		super(conf);
		realTimeDetection = true;
	}
	
	/**
	 * @param config
	 * @param distrFilePathParam
	 * @param hdfsFileParam
	 * @param schemaFilePathParam
	 * @param scoreThresholdParam
	 * @throws IOException
	 */
	public EstimatedProbabilityBasedPredictor(Map<String, Object> config, String idOrdinalsParam, 
			String distrFilePathParam, String hdfsFileParam, String schemaFilePathParam, 
			String seasonalParam, String fieldDelimParam, String scoreThresholdParam) throws IOException {
		super(config, idOrdinalsParam, distrFilePathParam, hdfsFileParam, schemaFilePathParam, 
				seasonalParam,  fieldDelimParam, scoreThresholdParam);
	}

	/**
	 * Hadoop MR usage
	 * @param config
	 * @param distrFilePath
	 * @throws IOException
	 */
	public EstimatedProbabilityBasedPredictor(Configuration config, String distrFilePath, String scoreThresholdParam) throws IOException {
		super(config, distrFilePath);
		scoreThreshold = Double.parseDouble( config.get( scoreThresholdParam));
	}
	
	@Override
	public double execute(String entityID, String record) {
		String bucketKey = getBucketKey(record);
		Integer count = distrModel.get(bucketKey);
		double pr = null != count ? (((double)count) / totalCount) : 0;
		double score = 1.0 - pr;
		scoreAboveThreshold = score > scoreThreshold;
		if (realTimeDetection && scoreAboveThreshold) {
			//write if above threshold
			outQueue.send(entityID + " " + score);
		}
		return score;
	}
	
	@Override
	public double execute(String[] items, String compKey) {
		String bucketKey = getBucketKey(items);
		Map<String, Integer> distrModel = keyedDistrModel.get(compKey);
		Integer count = distrModel.get(bucketKey);
		int totalCount = totalCounts.get(compKey);
		double pr = null != count ? (((double)count) / totalCount) : 0;
		double score = 1.0 - pr;
		return score;
	}
}
