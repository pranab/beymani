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
import org.chombo.storm.Cache;
import org.chombo.storm.MessageQueue;
import org.chombo.util.BasicUtils;
import org.chombo.util.ConfigUtility;
import org.chombo.stats.MedianStatsManager;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class RobustZscorePredictor extends ModelBasedPredictor {
	private int[] attrOrdinals;
    private MedianStatsManager medStatManager;
	private String fieldDelim;
	private double[] attrWeights;
	protected MessageQueue outQueue;
	protected Cache cache;
	
	/**
	 * Storm usage
	 * @param config
	 * @param idOrdinalsParam
	 * @param attrListParam
	 * @param fieldDelimParam
	 * @param attrWeightParam
	 * @param medModelKeyParam
	 * @param madModelKeyParam
	 * @param scoreThresholdParam
	 * @throws IOException
	 */
	public RobustZscorePredictor(Map config, String idOrdinalsParam, String attrListParam, String fieldDelimParam, 
			String attrWeightParam, String medModelKeyParam, String madModelKeyParam, String scoreThresholdParam, 
			boolean frmCache) 
		throws IOException {
		idOrdinals = ConfigUtility.getIntArray(config, idOrdinalsParam);
		attrOrdinals = ConfigUtility.getIntArray(config, attrListParam);
		fieldDelim = ConfigUtility.getString(config, fieldDelimParam, ",");

		outQueue = MessageQueue.createMessageQueue(config, config.get("output.queue").toString());
		cache = Cache.createCache(config);
		
		String medlKey =  config.get(medModelKeyParam).toString();
		String medContent = cache.get(medlKey);
		String madlKey =  config.get(madModelKeyParam).toString();
		String madContent = cache.get(madlKey);
		medStatManager=  new MedianStatsManager(medContent, madContent, ",", idOrdinals);

		attrWeights = ConfigUtility.getDoubleArray(config, attrWeightParam);
		scoreThreshold = ConfigUtility.getDouble(config, scoreThresholdParam, 3.0);
		realTimeDetection = true;
	}
	
	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param attrListParam
	 * @param medFilePathParam
	 * @param madFilePathParam
	 * @param fieldDelimParam
	 * @param attrWeightParam
	 * @param scoreThresholdParam
	 * @throws IOException
	 */
	public RobustZscorePredictor(Map<String, Object> config, String idOrdinalsParam, String attrListParam, 
			String medFilePathParam, String madFilePathParam,  String fieldDelimParam, String attrWeightParam, 
			String seasonalParam, String hdfsFileParam, String expConstParam, String scoreThresholdParam) throws IOException {
		idOrdinals = ConfigUtility.getIntArray(config, idOrdinalsParam);
		attrOrdinals = ConfigUtility.getIntArray(config, attrListParam);
		fieldDelim = ConfigUtility.getString(config, fieldDelimParam, ",");
		boolean hdfsFilePath = ConfigUtility.getBoolean(config, hdfsFileParam);
		String medFilePath = ConfigUtility.getString(config, medFilePathParam);
		String madFilePath = ConfigUtility.getString(config, medFilePathParam);
		boolean seasonal = ConfigUtility.getBoolean(config, seasonalParam);
		medStatManager = new MedianStatsManager(config, medFilePath, madFilePath, fieldDelim,  idOrdinals, hdfsFilePath,  seasonal);
		
		attrWeights = ConfigUtility.getDoubleArray(config, attrWeightParam);
		expConst = ConfigUtility.getDouble(config, expConstParam);
		scoreThreshold = ConfigUtility.getDouble(config, scoreThresholdParam);
	}

	/**
	 * Hadoop MR usage for robust zscore
	 * @param config
	 * @param idOrdinalsParam
	 * @param attrListParam
	 * @param medFilePathParam
	 * @param madFilePathParam
	 * @param fieldDelimParam
	 * @throws IOException
	 */
	public RobustZscorePredictor(Configuration config, String idOrdinalsParam, String attrListParam, 
			String medFilePathParam, String madFilePathParam,  String fieldDelimParam, String attrWeightParam, 
			String scoreThresholdParam, boolean seasonal) throws IOException {
		idOrdinals = Utility.intArrayFromString(config.get(idOrdinalsParam));
		attrOrdinals = Utility.intArrayFromString(config.get(attrListParam));
    	medStatManager = new MedianStatsManager(config, medFilePathParam, madFilePathParam,  
        		",",  idOrdinals,  seasonal);

		fieldDelim = config.get(fieldDelimParam, ",");
			
		//attribute weights
		attrWeights = Utility.doubleArrayFromString(config.get(attrWeightParam), fieldDelim);
		scoreThreshold = Double.parseDouble( config.get( scoreThresholdParam));
	}

	@Override
	public double execute(String entityID, String record) {
		double score = 0;
		String[] items = record.split(fieldDelim);
		int i = 0;
		double totalWt = 0;
		for (int ord  :  attrOrdinals) {
			double val = Double.parseDouble(items[ord]);
			if (null != idOrdinals) {
				String compKey = Utility.join(items, idOrdinals, fieldDelim);
				score  += (Math.abs( val - medStatManager.getKeyedMedian(compKey, ord) ) / 
						medStatManager.getKeyedMedAbsDivergence(compKey, ord)) * attrWeights[i];
			}	else {
				score  += (Math.abs( val -  medStatManager.getMedian(ord)) / medStatManager.getMedAbsDivergence(ord)) * attrWeights[i];
			}
		}
		score /=  totalWt ;
		scoreAboveThreshold = score > scoreThreshold;
		if (realTimeDetection && scoreAboveThreshold) {
			//write if above threshold
			outQueue.send(entityID + " " + score);
		}
		return score;
	}
	
	@Override
	public double execute(String[] items, String compKey) {
		double score = 0;
		int i = 0;
		double totalWt = 0;
		for (int ord  :  attrOrdinals) {
			double val = Double.parseDouble(items[ord]);
			if (null != idOrdinals) {
				score  += (Math.abs( val - medStatManager.getKeyedMedian(compKey, ord) ) / 
						medStatManager.getKeyedMedAbsDivergence(compKey, ord)) * attrWeights[i];
			}	else {
				score  += (Math.abs( val -  medStatManager.getMedian(ord)) / medStatManager.getMedAbsDivergence(ord)) * attrWeights[i];
			}
		}
		score /=  totalWt ;
		
		//exponential normalization
		score = BasicUtils.expScale(expConst, score);

		scoreAboveThreshold = score > scoreThreshold;
		return score;
		
	}	
}
