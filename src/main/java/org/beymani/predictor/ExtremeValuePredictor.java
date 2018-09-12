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

/**
 * @author pranab
 *
 */
public class ExtremeValuePredictor extends ZscorePredictor {

	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param attrListParam
	 * @param fieldDelimParam
	 * @param attrWeightParam
	 * @param statsFilePathParam
	 * @param seasonalParam
	 * @param hdfsFileParam
	 * @param scoreThresholdParam
	 * @param expConstParam
	 * @throws IOException
	 */
	public ExtremeValuePredictor(Map<String, Object> config,String idOrdinalsParam, String attrListParam,
			String fieldDelimParam, String attrWeightParam,String statsFilePathParam, String seasonalParam,
			String hdfsFileParam, String scoreThresholdParam,String expConstParam) throws IOException {
		super(config, idOrdinalsParam, attrListParam, fieldDelimParam, attrWeightParam,
				statsFilePathParam, seasonalParam, hdfsFileParam, scoreThresholdParam,
				expConstParam);
	}

	/* (non-Javadoc)
	 * @see org.beymani.predictor.ZscorePredictor#execute(java.lang.String[], java.lang.String)
	 */
	@Override
	public double execute(String[] items, String compKey) {
		double score = 0;
		int i = 0;
		double totalWt = 0;
		for (int ord  :  attrOrdinals) {
			double val = Double.parseDouble(items[ord]);
			double d = 0;
			double e = 0;
			if (null != idOrdinals) {
				d = Math.abs( val - statsManager.getMean(compKey,ord));
				e = Math.exp(-d / statsManager.getStdDev(compKey, ord));
			} else {
				d = Math.abs( val - statsManager.getMean(ord));
				e = Math.exp(-d / statsManager.getStdDev(ord));
			}
			score  += Math.exp(-e) * attrWeights[i];
			totalWt += attrWeights[i];
			++i;
		}
		score /=  totalWt ;
		
		scoreAboveThreshold = score > scoreThreshold;
		return score;
	}
	
}
