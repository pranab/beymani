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
import org.chombo.stats.HistogramStat;
import org.chombo.util.BasicUtils;

/**
 * Based on probability of probability p(f(y) < f(x)). f(x) is density function
 * @author pranab
 *
 */
public class EstimatedMetaProbabilityBasedPredictor extends EsimatedAttrtibuteProbabilityBasedPredictor {

	public EstimatedMetaProbabilityBasedPredictor(Map conf) {
		super(conf);
	}

	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param attrListParam
	 * @param distrFilePathParam
	 * @param hdfsFileParam
	 * @param schemaFilePathParam
	 * @param attrWeightParam
	 * @param seasonalParam
	 * @param fieldDelimParam
	 * @param scoreThresholdParam
	 * @param ignoreMissingDistrParam
	 * @throws IOException
	 */
	public EstimatedMetaProbabilityBasedPredictor(Map<String, Object> config,String idOrdinalsParam, String attrListParam,
			String distrFilePathParam, String hdfsFileParam,String schemaFilePathParam, String attrWeightParam,
			String seasonalParam, String fieldDelimParam,String scoreThresholdParam, String ignoreMissingDistrParam, 
			String scoreStrategyParam, String expConstParam, String scoreAggggregationStrtaegyParam)
			throws IOException {
		super(config, idOrdinalsParam, attrListParam, distrFilePathParam,hdfsFileParam, schemaFilePathParam, attrWeightParam,
				seasonalParam, fieldDelimParam, scoreThresholdParam,ignoreMissingDistrParam, scoreStrategyParam, expConstParam,
				scoreAggggregationStrtaegyParam);
	}

	/**
	 * @param config
	 * @param distrFilePathParam
	 * @param attrWeightParam
	 * @param scoreThresholdParam
	 * @param fieldDelimParam
	 * @throws IOException
	 */
	public EstimatedMetaProbabilityBasedPredictor(Configuration config,String distrFilePathParam, String attrWeightParam,
			String scoreThresholdParam, String fieldDelimParam)
			throws IOException {
		super(config, distrFilePathParam, attrWeightParam, scoreThresholdParam,fieldDelimParam);
	}
	
	@Override
	public double execute(String[] items, String compKey) {
		double score = 0;
		int i = 0;
		double totalWt = 0;
		int validCount = 0;
		for (int ord  :  attrOrdinals) {
			String keyWithFldOrd = compKey + fieldDelim + ord;
			double val = Double.parseDouble(items[ord]);
			System.out.println("keyWithFldOrd " + keyWithFldOrd);
			HistogramStat hist = keyedHist.get(keyWithFldOrd);
			if (null != hist) {
				double distr = hist.findMetaDistr(val);
				double thisScore = 0;
				if (scoreStrategy.equals("inverse")) {
					thisScore = 1.0 - distr;
				} else {
					if (distr > 0) {
						thisScore = -Math.log(distr);
					} else {
						thisScore = 20.0;
					}
				}
				score += thisScore * attrWeights[i];
				totalWt += attrWeights[i];
				++validCount;
			} else {
				BasicUtils.assertCondition(!ignoreMissingDistr, "missing distr for key " + keyWithFldOrd);
			}
			++i;
		}
		if (validCount > 0) {
			score /=  totalWt ;
		} 
		
		//exponential normalization
		if (expConst > 0) {
			score = BasicUtils.expScale(expConst, score);
		}
		
		scoreAboveThreshold = score > scoreThreshold;
		return score;
	}
	
}
