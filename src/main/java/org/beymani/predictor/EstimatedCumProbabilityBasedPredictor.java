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
import org.beymani.util.OutlierScoreAggregator;
import org.chombo.stats.HistogramStat;
import org.chombo.util.BasicUtils;

public class EstimatedCumProbabilityBasedPredictor extends EsimatedAttrtibuteProbabilityBasedPredictor {

	public EstimatedCumProbabilityBasedPredictor(Map conf) {
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
	public EstimatedCumProbabilityBasedPredictor(Map<String, Object> config,String idOrdinalsParam, String attrListParam,
			String distrFilePathParam, String hdfsFileParam,String schemaFilePathParam, String attrWeightParam,
			String seasonalParam, String fieldDelimParam,String scoreThresholdParam, String ignoreMissingDistrParam,
			String scoreAggggregationStrtaegyParam)
			throws IOException {
		super(config, idOrdinalsParam, attrListParam, distrFilePathParam,hdfsFileParam, schemaFilePathParam, attrWeightParam,
				seasonalParam, fieldDelimParam, scoreThresholdParam,ignoreMissingDistrParam, "score.strategy", "exp.const",
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
	public EstimatedCumProbabilityBasedPredictor(Configuration config,String distrFilePathParam, String attrWeightParam,
			String scoreThresholdParam, String fieldDelimParam)
			throws IOException {
		super(config, distrFilePathParam, attrWeightParam, scoreThresholdParam,fieldDelimParam);
	}

	@Override
	public double execute(String[] items, String compKey) {
		double score = 0;
		OutlierScoreAggregator scoreAggregator = new OutlierScoreAggregator(attrWeights.length, attrWeights);
		double thisScore = 0;
		for (int ord  :  attrOrdinals) {
			String keyWithFldOrd = compKey + fieldDelim + ord;
			double val = Double.parseDouble(items[ord]);
			System.out.println("keyWithFldOrd " + keyWithFldOrd);
			HistogramStat hist = keyedHist.get(keyWithFldOrd);
			if (null != hist) {
				double distr = hist.findCumDistr(val);
				thisScore = distr < 0.5 ? 1.0 - distr : distr;
				scoreAggregator.addScore(thisScore);
			} else {
				BasicUtils.assertCondition(!ignoreMissingDistr, "missing distr for key " + keyWithFldOrd);
				scoreAggregator.addScore();
			}
		}
		//aggregate score	
		score = getAggregateScore(scoreAggregator);
		
		scoreAboveThreshold = score > scoreThreshold;
		return score;
	}
	
}
