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
import org.chombo.util.MedianStatsManager;
import org.chombo.util.NumericalAttrStatsManager;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class ZscorePredictor  extends ModelBasedPredictor{
	private int[] idOrdinals;
	private int[] attrOrdinals;
	private NumericalAttrStatsManager statsManager;
    private MedianStatsManager medStatManager;
	private String fieldDelim;
	private boolean robustZscore;
	private double[] attrWeights;
	
	/**
	 * @param conf
	 */
	public ZscorePredictor(Map conf) {
		//TODO
	}
	
	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param asttrListParam
	 * @param statsFilePath
	 * @param schemaFilePath
	 * @param fieldDelimParam
	 * @throws IOException
	 */
	public ZscorePredictor(Configuration config, String idOrdinalsParam, String attrListParam, 
		String statsFilePath,  String fieldDelimParam) throws IOException {
		idOrdinals = Utility.intArrayFromString(config.get(idOrdinalsParam));
		attrOrdinals = Utility.intArrayFromString(config.get(attrListParam));
		statsManager = new NumericalAttrStatsManager(config, statsFilePath, ",");
		fieldDelim = config.get(fieldDelimParam, ",");
		
		//attribute weights
		String[] weightStrs =  config.get("attr.weight").split(",");
		attrWeights = new double[weightStrs.length];
		for (int a = 0; a < weightStrs.length; ++a) {
			attrWeights[a] = Double.parseDouble(weightStrs[a]);
		}
	}
	
	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param attrListParam
	 * @param medFilePathParam
	 * @param madFilePathParam
	 * @param fieldDelimParam
	 * @throws IOException
	 */
	public ZscorePredictor(Configuration config, String idOrdinalsParam, String attrListParam, 
			String medFilePathParam, String madFilePathParam,  String fieldDelimParam) throws IOException {
			idOrdinals = Utility.intArrayFromString(config.get(idOrdinalsParam));
			attrOrdinals = Utility.intArrayFromString(config.get(attrListParam));
    		medStatManager = new MedianStatsManager(config, medFilePathParam, madFilePathParam,  
        			",",  idOrdinals);

			fieldDelim = config.get(fieldDelimParam, ",");
			
			//attribute weights
			String[] weightStrs =  config.get("attr.weight").split(",");
			attrWeights = new double[weightStrs.length];
			for (int a = 0; a < weightStrs.length; ++a) {
				attrWeights[a] = Double.parseDouble(weightStrs[a]);
			}
			robustZscore = true;
		}

	@Override
	public double execute(String entityID, String record) {
		double score = 0;
		
		String[] items = record.split(fieldDelim);
		int i = 0;
		double totalWt = 0;
		for (int ord  :  attrOrdinals) {
			double val = Double.parseDouble(items[ord]);
			if (robustZscore) {
				if (null != idOrdinals) {
					String compKey = Utility.join(items, idOrdinals, fieldDelim);
					score  += (Math.abs( val - medStatManager.getKeyedMedian(compKey, ord) ) / 
							medStatManager.getKeyedMedAbsDivergence(compKey, ord)) * attrWeights[i];
				}	else {
					score  += (Math.abs( val -  medStatManager.getMedian(ord)) / medStatManager.getMedAbsDivergence(ord)) * attrWeights[i];
				}
			} else {
				if (null != idOrdinals) {
					String compKey = Utility.join(items, idOrdinals, fieldDelim);
					score  += (Math.abs( val -  statsManager.getMean(compKey,ord)) / statsManager.getStdDev(compKey, ord)) * attrWeights[i];
				} else {
					score  += (Math.abs( val -  statsManager.getMean(ord)) / statsManager.getStdDev(ord)) * attrWeights[i];
				}
			}
			totalWt += attrWeights[i];
			++i;
		}
		return score / totalWt ;
	}

}
