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
 * Predict outlier based on increase of entropy resulting from including outlier point
 * @author pranab
 *
 */
public class EntropyIncreaseBasedPredictor extends DistributionBasedPredictor {
	private double entropy;
	private double baseConvConst = Math.log(2);
	private String subFieldDelim = ":";
	
	public EntropyIncreaseBasedPredictor(Map conf) {
		super(conf);
		
		//entropy
		entropy = 0;
		for (String bucketKey : distrModel.keySet()) {
			double pr = ((double)distrModel.get(bucketKey)) / totalCount;
			entropy += -pr * Math.log(pr) / baseConvConst;
		}
	}

	@Override
	public double execute(String entityID, String record) {
		double score = 0;
		String thisBucketKey = getBucketKey(record);
		
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
		
		if (score > scoreThreshold) {
			//write if above threshold
			outQueue.send(entityID + " " + score);
		}
		return score;
	}

	@Override
	public double execute(String[] items, String compKey) {
		//TODO
		double score = 0;
		
		return score;
	}
	
}
