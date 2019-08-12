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

import java.io.Serializable;
import java.util.Map;

import org.beymani.util.OutlierScoreAggregator;
import org.chombo.util.BasicUtils;
import org.chombo.util.ConfigUtility;

/**
 * Base class for all model based predictors
 * @author pranab
 *
 */
public abstract class ModelBasedPredictor implements Serializable {
	protected boolean realTimeDetection;
	protected double scoreThreshold;
	protected boolean scoreAboveThreshold;
	protected boolean partition = false;
	protected double expConst = 1.0;
	protected int[] idOrdinals;
	protected int[] attrOrdinals;
	protected double[] attrWeights;
	protected boolean ignoreMissingStat;
	private String aggregationStrategy;

	public ModelBasedPredictor() {
		
	}
	
	/**
	 * @param config
	 * @param attrWeightParam
	 * @param scoreAggggregationStrtaegyParam
	 */
	public ModelBasedPredictor(Map<String, Object> config, String attrWeightParam, String scoreAggggregationStrtaegyParam) {
		attrWeights = ConfigUtility.getDoubleArray(config, attrWeightParam);
		aggregationStrategy = ConfigUtility.getString(config, scoreAggggregationStrtaegyParam);;
	}
	
	/**
	 * @param entityID
	 * @param record
	 * @return
	 */
	public abstract double execute(String entityID, String record);
	
	/**
	 * @param items
	 * @param compKey
	 * @return
	 */
	public abstract double execute(String[] items, String compKey);


	/**
	 * @return
	 */
	public boolean isScoreAboveThreshold() {
		return scoreAboveThreshold;
	} 
	
	/**
	 * @return
	 */
	public ModelBasedPredictor withPartition() {
		partition = true;
		return this;
	}

	/**
	 * @param ignoreMissingStat
	 * @return
	 */
	public ModelBasedPredictor withIgnoreMissingStat(boolean ignoreMissingStat) {
		this.ignoreMissingStat = ignoreMissingStat;
		return this;
	}

	
	/**
	 * @param compKey
	 * @return
	 */
	public  abstract boolean isValid(String compKey);
	
	/**
	 * @return
	 */
	public double getAggregateScore(OutlierScoreAggregator scoreAggregator) {
		double aggrScore = 0;
		if (aggregationStrategy.equals("average")) {
			aggrScore = scoreAggregator.getAverage();
		} else if (aggregationStrategy.equals("weightedAverage")) {
			aggrScore = scoreAggregator.getWeightedAverage();
		} else if (aggregationStrategy.equals("median")) {
			aggrScore = scoreAggregator.getMedian();
		} else {
			BasicUtils.assertFail("invalid outlier score aggregation strategy " + aggregationStrategy);
		}
		return aggrScore;
	}
	
}
