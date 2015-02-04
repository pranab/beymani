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

/**
 * Estimated probability based outlier prediction
 * @author pranab
 *
 */
public class EstimatedProbabilityBasedPredictor extends DistributionBasedPredictor {

	public EstimatedProbabilityBasedPredictor(Map conf) {
		super(conf);
	}

	@Override
	public double execute(String entityID, String record) {
		String bucketKey = getBucketKey(record);
		Integer count = distrModel.get(bucketKey);
		double pr = null != count ? (((double)count) / totalCount) : 0;
		double score = 1.0 - pr;
		if (score > scoreThreshold) {
			//write if above threshold
			outQueue.send(entityID + " " + score);
		}
		return score;
	}

}
