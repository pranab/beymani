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

import org.chombo.math.MathUtils;
import org.chombo.stats.MultiVariateStatsManager;
import org.chombo.util.BasicUtils;
import org.chombo.util.ConfigUtility;

import Jama.Matrix;


/**
 * Predictor based on Mahalanobis distance for multi variate data
 * @author pranab
 *
 */
public class MahalanobisDistancePredictor extends ModelBasedPredictor {
	private  MultiVariateStatsManager statsManager;
	
	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param attrListParam
	 * @param fieldDelimParam
	 * @param statsFilePathParam
	 * @param seasonalParam
	 * @param hdfsFileParam
	 * @param scoreThresholdParam
	 * @param expConstParam
	 * @param ignoreMissingStatParam
	 * @param scoreAggggregationStrtaegyParam
	 * @throws IOException
	 */
	public MahalanobisDistancePredictor(Map<String, Object> config, String idOrdinalsParam, String attrListParam, 
			String fieldDelimParam,  String statsFilePathParam, String seasonalParam,String hdfsFileParam, 
			String scoreThresholdParam, String expConstParam, String ignoreMissingStatParam) 
		throws IOException {
		idOrdinals = ConfigUtility.getIntArray(config, idOrdinalsParam);
		attrOrdinals = ConfigUtility.getIntArray(config, attrListParam);
		fieldDelim = ConfigUtility.getString(config, fieldDelimParam, ",");
		
		String statsFilePath = ConfigUtility.getString(config, statsFilePathParam);
		boolean hdfsFilePath = ConfigUtility.getBoolean(config, hdfsFileParam);
		seasonal = ConfigUtility.getBoolean(config, seasonalParam);
		statsManager = new MultiVariateStatsManager(statsFilePath, fieldDelim, hdfsFilePath);
		scoreThreshold = ConfigUtility.getDouble(config, scoreThresholdParam);
		realTimeDetection = true;
		expConst = ConfigUtility.getDouble(config, expConstParam);
		ignoreMissingStat = ConfigUtility.getBoolean(config, ignoreMissingStatParam);
	}

	@Override
	public double execute(String entityID, String record) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double execute(String[] items, String compKey) {
		double score = 0;
		if (statsManager.statsExists(compKey)) {
			//extract input vector and subtract mean vector
			double[] data = BasicUtils.extractFieldsAsDoubleArray(items , attrOrdinals);
			Matrix input = MathUtils.createRowMatrix(data);
			Matrix inputOffset = MathUtils.subtractMatrix(input, statsManager.getMeanVec(compKey));
			Matrix inputOffsetTr = MathUtils.transposeMatrix(inputOffset);
			
			
			//mahalanobis distance
			Matrix invCovar = statsManager.getInvCoVarMatrix(compKey);
			Matrix maDist = MathUtils.multiplyMatrix(inputOffset, invCovar);
			maDist = MathUtils.multiplyMatrix(maDist, inputOffsetTr);
			score = MathUtils.scalarFromMatrix(maDist);
		} else {
			BasicUtils.assertCondition(!ignoreMissingStat, "missing stats for key " + compKey );
		}
		
		//exponential normalization
		if (expConst > 0) {
			score = BasicUtils.expScale(expConst, score);
		}

		scoreAboveThreshold = score > scoreThreshold;
		return score;
	}

	@Override
	public boolean isValid(String compKey) {
		return statsManager.statsExists(compKey);
	}

}
