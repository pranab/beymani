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

/**
 * Inter percentile difference (25% and 75%) based predictor
 * @author pranab
 *
 */
public class InterPercentileDifferenceBasedPredictor extends EsimatedAttrtibuteProbabilityBasedPredictor {
	private static final int QUARTER_PERECENTILE = 25;
	private static final int THREE_QUARTER_PERECENTILE = 75;
	
	/**
	 * @param conf
	 */
	public InterPercentileDifferenceBasedPredictor(Map conf) {
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
	public InterPercentileDifferenceBasedPredictor(Map<String, Object> config,String idOrdinalsParam, String attrListParam,
			String distrFilePathParam, String hdfsFileParam,String schemaFilePathParam, String attrWeightParam,
			String seasonalParam, String fieldDelimParam,String scoreThresholdParam, String ignoreMissingDistrParam,
			String expConstParam, String scoreAggggregationStrtaegyParam)
			throws IOException {
		super(config, idOrdinalsParam, attrListParam, distrFilePathParam,hdfsFileParam, schemaFilePathParam, attrWeightParam,
				seasonalParam, fieldDelimParam, scoreThresholdParam,ignoreMissingDistrParam, "score.strategy", expConstParam,
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
	public InterPercentileDifferenceBasedPredictor(Configuration config,String distrFilePathParam, String attrWeightParam,
			String scoreThresholdParam, String fieldDelimParam)
			throws IOException {
		super(config, distrFilePathParam, attrWeightParam, scoreThresholdParam,fieldDelimParam);
	}

	/* (non-Javadoc)
	 * @see org.beymani.predictor.EsimatedAttrtibuteProbabilityBasedPredictor#execute(java.lang.String[], java.lang.String)
	 */
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
				double thisScore = 0;
				double quarterPercentile = hist.getQuantile(QUARTER_PERECENTILE);
				double threeQuarterPercentile = hist.getQuantile(THREE_QUARTER_PERECENTILE); 
				double percentileDiff = threeQuarterPercentile - quarterPercentile;
				if (val < quarterPercentile) {
					thisScore = (quarterPercentile - val) / percentileDiff;
				} else if (val > threeQuarterPercentile){
					thisScore = (val - threeQuarterPercentile) / percentileDiff;
				}
				score += thisScore * attrWeights[i];
				totalWt += attrWeights[i];
				++validCount;
			} else {
				if (!ignoreMissingDistr) {
					throw new IllegalStateException("missing distr for key " + keyWithFldOrd);
				}
			}
			++i;
		}
		if (validCount > 0) {
			score /=  totalWt ;
		} 
		
		scoreAboveThreshold = score > scoreThreshold;
		return score;
	}
	
}
