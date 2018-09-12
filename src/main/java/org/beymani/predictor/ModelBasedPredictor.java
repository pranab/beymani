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
	protected double expConst;
	protected int[] idOrdinals;
	protected int[] attrOrdinals;
	protected double[] attrWeights;

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
}
