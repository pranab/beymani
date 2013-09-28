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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.chombo.util.Pair;
import org.chombo.util.Utility;

import redis.clients.jedis.Jedis;

/**
 * Predictor based on markov model
 * @author pranab
 *
 */
public class MarkovModelPredictor extends ModelBasedPredictor {
	private List<String> states;
	private double[][] stateTranstionProb;
	private Map<String, List<String>> records = new HashMap<String, List<String>>(); 
	private boolean localPredictor;
	private int stateSeqWindowSize;
	private int stateOrdinal;
	private enum DetectionAlgorithm {
		MissProbability, 
		MissRate, 
		EntropyReduction
	};
	private 	DetectionAlgorithm detectionAlgorithm;
	private Map<String, Pair<Double, Double>> globalParams;
	private double metricThreshold;
	
	/**
	 * @param conf
	 */
	public MarkovModelPredictor(Map conf)   {
		String redisHost = conf.get("redis.server.host").toString();
		int redisPort = new Integer(conf.get("redis.server.port").toString());
		Jedis jedis = new Jedis(redisHost, redisPort);
		String modelKey =  conf.get("redis.markov.model.key").toString();
		String model = jedis.get(modelKey);
		
		Scanner scanner = new Scanner(model);
		int lineCount = 0;
		int numStates = 0;
		int row = 0;
		while (scanner.hasNextLine()) {
		  String line = scanner.nextLine();
		  if (0 == lineCount) {
			  //states
			  String[] items = line.split(",");
			  states = Arrays.asList(items);
			  numStates = items.length;
			  stateTranstionProb = new double[numStates][numStates];
		  } else {
			  //populate state transtion probability
		        Utility.deseralizeTableRow(stateTranstionProb, line, ",", row, numStates);
		        ++row;
		  }
		  ++lineCount;
		}
		 scanner.close();
		 localPredictor = Boolean.parseBoolean(conf.get("local.predictor").toString());
		 if (localPredictor) {
			 stateSeqWindowSize =  Integer.parseInt(conf.get("state.seq.window.size").toString());
		 }  else {
			 stateSeqWindowSize = 5;
			 globalParams = new HashMap<String, Pair<Double, Double>>();
		 }
		 //state value ordinal within record
		 stateOrdinal =  Integer.parseInt(conf.get("state.ordinal").toString());
		 
		 //detection algoritm
		 String algorithm = conf.get("detection.algorithms").toString();
		 if (algorithm.equals("missProbability")) {
			 detectionAlgorithm = DetectionAlgorithm.MissProbability;
		 } else if (algorithm.equals("missRate")) {
			 detectionAlgorithm = DetectionAlgorithm.MissRate;
		 } else if (algorithm.equals("entropyReduction")) {
			 detectionAlgorithm = DetectionAlgorithm.EntropyReduction;
		 } else {
			 //error
		 }
		 
		 //metric threshold
		 metricThreshold =  Double.parseDouble(conf.get("metric.threshold").toString());
	}

	@Override
	public double execute(String entityID, String record) {
		double score = 0;
		
		List<String> recordSeq = records.get(entityID);
		if (null == recordSeq) {
			recordSeq = new ArrayList<String>();
		}
		recordSeq.add(record);

		if (recordSeq.size() > stateSeqWindowSize) {
			records.remove(0);
		}
		
		if (localPredictor) {
			//local metric
			if (recordSeq.size() == stateSeqWindowSize) {
				String[] stateSeq = new String[stateSeqWindowSize]; 
				for (int i = 0; i < stateSeqWindowSize; ++i) {
					stateSeq[i++] = recordSeq.get(i).split(",")[stateOrdinal];
				}
				score = getLocalMetric( stateSeq);
			}
		} else {
			//global metric
			if (recordSeq.size() >= 2) {
				String[] stateSeq = new String[2];
				for (int i = stateSeqWindowSize - 2, j =0; i < stateSeqWindowSize; ++i) {
					stateSeq[j++] = recordSeq.get(i).split(",")[stateOrdinal];
				}
				Pair<Double,Double> params = globalParams.get(entityID);
				if (null == params) {
					params = new Pair<Double,Double>(0.0, 0.0);
					globalParams.put(entityID, params);
				}
				score = getGlobalMetric( stateSeq, params);
			}
		}		
		
		//outlier
		if (score > metricThreshold) {
			
		}
		return score;
	}
	

	/**
	 * @param stateSeq
	 * @return
	 */
	private double getLocalMetric(String[] stateSeq) {
		double metric = 0;
		double paramF = 0;
		double paramG = 0;
		if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
			for (int i = 1; i < stateSeq.length; ++i ){
				int prState = states.indexOf(stateSeq[i -1]);
				int cuState = states.indexOf(stateSeq[i ]);
				
				//add all probability except target state
				for (int j = 0; j < states.size(); ++ j) {
					if (j != cuState)
					paramF += stateTranstionProb[prState][j];
				}
				paramG += 1;
			}
		} else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
			for (int i = 1; i < stateSeq.length; ++i ){
				int prState = states.indexOf(stateSeq[i -1]);
				int cuState = states.indexOf(stateSeq[i ]);
				
				//if target is max prob state then 0 else 1
				double maxProb = 0;
				int maxprobStateIndex = - 1;
				for (int j = 0; j < states.size(); ++ j) {
					if ( stateTranstionProb[prState][j] > maxProb) {
						maxProb = stateTranstionProb[prState][j];
						maxprobStateIndex = i;
					}
				}
				paramF += (cuState == maxprobStateIndex? 0 : 1);
				paramG += 1;
			}
			
		}
		
		metric = paramF / paramG;	
		return metric;
		
	}	
	

	/**
	 * @param stateSeq
	 * @return
	 */
	private double getGlobalMetric(String[] stateSeq, Pair<Double,Double> params) {
		double metric = 0;
		double paramF = 0;
		double paramG = 0;
		if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
			for (int i = 1; i < stateSeq.length; ++i ){
				int prState = states.indexOf(stateSeq[i -1]);
				int cuState = states.indexOf(stateSeq[i ]);
				for (int j = 0; j < states.size(); ++ j) {
					if (j != cuState)
					paramF += stateTranstionProb[prState][j];
				}
				paramG += 1;
			}
		} else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
			for (int i = 1; i < stateSeq.length; ++i ){
				int prState = states.indexOf(stateSeq[i -1]);
				int cuState = states.indexOf(stateSeq[i ]);
				
				//if target is max prob state then 0 else 1
				double maxProb = 0;
				int maxprobStateIndex = - 1;
				for (int j = 0; j < states.size(); ++ j) {
					if ( stateTranstionProb[prState][j] > maxProb) {
						maxProb = stateTranstionProb[prState][j];
						maxprobStateIndex = i;
					}
				}
				paramF += (cuState == maxprobStateIndex? 0 : 1);
				paramG += 1;
			}
		}
		params.setLeft(params.getLeft() + paramF);
		params.setRight(params.getRight() + paramG);
		
		metric = params.getLeft() / params.getRight();	
		return metric;
		
	}	
	
}
