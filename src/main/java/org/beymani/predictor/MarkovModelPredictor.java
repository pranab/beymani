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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.Cache;
import org.chombo.storm.MessageQueue;
import org.chombo.util.Pair;

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
	private DetectionAlgorithm detectionAlgorithm;
	private Map<String, Pair<Double, Double>> globalParams;
	private double metricThreshold;
	private int numStates;
	private int[] maxStateProbIndex;
	private double[] entropy;
	private String outputQueue;
	private MessageQueue outQueue;
	private Cache cache;
	private static final Logger LOG = Logger.getLogger(MarkovModelPredictor.class);
	private boolean debugOn;
	
	/**
	 * @param conf
	 */
	public MarkovModelPredictor(Map conf)   {
		if (conf.get("debug").toString().equals("on")) {
			LOG.setLevel(Level.DEBUG);;
			debugOn = true;
		}
		
		outputQueue =  conf.get("redis.output.queue").toString();
		outQueue = MessageQueue.createMessageQueue(conf, outputQueue);
		cache = Cache.createCache(conf);
		
		//model
		String modelKey =  conf.get("redis.markov.model.key").toString();
		String model = cache.get(modelKey);
		Scanner scanner = new Scanner(model);
		int lineCount = 0;
		int row = 0;
		while (scanner.hasNextLine()) {
		  String line = scanner.nextLine();
		  if (0 == lineCount) {
			  //states
			  String[] items = line.split(",");
			  states = Arrays.asList(items);
			  numStates = items.length;
			  stateTranstionProb = new double[numStates][numStates];
			  LOG.info("numStates:" + numStates);
		  } else {
			  //populate state transtion probability
		        deseralizeTableRow(stateTranstionProb, line, ",", row, numStates);
		        ++row;
		  }
		  ++lineCount;
		}
		 scanner.close();
		 if (debugOn){
			 for (int i = 0; i < numStates; ++i) {
				 for (int j = 0; j < numStates; ++j) {
					 LOG.info("state trans prob[" + i + "][" + j  +"]=" +  stateTranstionProb[i][j]);
				 }
			 }
		 }
		 
		 localPredictor = Boolean.parseBoolean(conf.get("local.predictor").toString());
		 if (localPredictor) {
			 stateSeqWindowSize =  Integer.parseInt(conf.get("state.seq.window.size").toString());
			 if (debugOn)
				 LOG.info("local predictor window size:" + stateSeqWindowSize );
		 }  else {
			 stateSeqWindowSize = 5;
			 globalParams = new HashMap<String, Pair<Double, Double>>();
		 }
		 //state value ordinal within record
		 stateOrdinal =  Integer.parseInt(conf.get("state.ordinal").toString());
		 
		 //detection algoritm
		 String algorithm = conf.get("detection.algorithm").toString();
		 if (debugOn)
			 LOG.info("detection algorithm:" + algorithm);
		 if (algorithm.equals("missProbability")) {
			 detectionAlgorithm = DetectionAlgorithm.MissProbability;
		 } else if (algorithm.equals("missRate")) {
			 detectionAlgorithm = DetectionAlgorithm.MissRate;
			 
			 //max probability state index
			 maxStateProbIndex = new int[numStates];
			 for (int i = 0; i < numStates; ++i) {
				 int maxProbIndex = -1;
				 double maxProb = -1;
				 for (int j = 0; j < numStates; ++j) {
					 if (stateTranstionProb[i][j] > maxProb) {
						 maxProb = stateTranstionProb[i][j];
						 maxProbIndex = j;
					 }
				 }
				 maxStateProbIndex[i] = maxProbIndex;
			 }
		 } else if (algorithm.equals("entropyReduction")) {
			 detectionAlgorithm = DetectionAlgorithm.EntropyReduction;
			 
			 //entropy per source state
			 entropy = new double[numStates];
			 for (int i = 0; i < numStates; ++i) {
				 double ent = 0;
				 for (int j = 0; j < numStates; ++j) {
					 ent  += -stateTranstionProb[i][j] * Math.log(stateTranstionProb[i][j]);
				 }
				 entropy[i] = ent;
			 }
		 } else {
			 //error
		 }
		 
		 //metric threshold
		 metricThreshold =  Double.parseDouble(conf.get("metric.threshold").toString());
	}

	/**
	 * @param table
	 * @param data
	 * @param delim
	 * @param row
	 * @param numCol
	 */
	public  void deseralizeTableRow(double[][] table, String data, String delim, int row, int numCol) {
		String[] items = data.split(delim);
		if (items.length != numCol) {
			throw new IllegalArgumentException(
					"Row serialization failed, number of tokens in string does not match with number of columns");
		}
		for (int c = 0; c < numCol; ++c) {
				table[row][c]  = Double.parseDouble(items[c]);
		}
	}
	
	@Override
	public double execute(String entityID, String record) {
		double score = 0;
		
		List<String> recordSeq = records.get(entityID);
		if (null == recordSeq) {
			recordSeq = new ArrayList<String>();
			records.put(entityID, recordSeq);
		}
		
		//add and maintain size
		recordSeq.add(record);
		if (recordSeq.size() > stateSeqWindowSize) {
			recordSeq.remove(0);
		}
		
		String[] stateSeq = null;
		if (localPredictor) {
			//local metric
			if (debugOn)
				LOG.info("local metric,  seq size " + recordSeq.size());
			if (recordSeq.size() == stateSeqWindowSize) {
				stateSeq = new String[stateSeqWindowSize]; 
				for (int i = 0; i < stateSeqWindowSize; ++i) {
					stateSeq[i] = recordSeq.get(i).split(",")[stateOrdinal];
				}
				score = getLocalMetric( stateSeq);
			}
		} else {
			//global metric
			if (debugOn)
				LOG.info("global metric");
			if (recordSeq.size() >= 2) {
				stateSeq = new String[2];
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
		if (debugOn)
			LOG.info("metric  " + entityID + ":" + score);
		if (score > metricThreshold) {
			StringBuilder stBld = new StringBuilder(entityID);
			stBld.append(" : ");
			for (String st : stateSeq) {
				stBld.append(st).append(" ");
			}
			stBld.append(": ");
			stBld.append(score);
			outQueue.send(stBld.toString());
		}
		return score;
	}
	
	@Override
	public double execute(String[] items, String compKey) {
		//TODO
		double score = 0;
		
		return score;
	}

	/**
	 * @param stateSeq
	 * @return
	 */
	private double getLocalMetric(String[] stateSeq) {
		double metric = 0;
		double[] params = new double[2];
		params[0] = params[1] = 0;
		if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
			missProbability(stateSeq, params);
		} else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
			 missRate(stateSeq, params);
		} else {
			 entropyReduction( stateSeq, params);
		}
		metric = params[0] / params[1];	
		return metric;
	}	
	

	/**
	 * @param stateSeq
	 * @return
	 */
	private double getGlobalMetric(String[] stateSeq, Pair<Double,Double> globParams) {
		double metric = 0;
		double[] params = new double[2];
		params[0] = params[1] = 0;
		if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
			missProbability(stateSeq, params);
		} else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
			 missRate(stateSeq, params);
		} else {
			 entropyReduction( stateSeq, params);
		}
		
		globParams.setLeft(globParams.getLeft() + params[0]);
		globParams.setRight(globParams.getRight() + params[1]);
		metric = globParams.getLeft() / globParams.getRight();	
		return metric;
	}	

	/**
	 * @param stateSeq
	 * @return
	 */
	private void missProbability(String[] stateSeq, double[] params) {
		int start = localPredictor? 1 :  stateSeq.length - 1;
		for (int i = start; i < stateSeq.length; ++i ){
			int prState = states.indexOf(stateSeq[i -1]);
			int cuState = states.indexOf(stateSeq[i ]);
			if (debugOn)
				LOG.info("state prob index:" + prState + " " + cuState);
			
			//add all probability except target state
			for (int j = 0; j < states.size(); ++ j) {
				if (j != cuState)
					params[0] += stateTranstionProb[prState][j];
			}
			params[1] += 1;
		}
		if (debugOn)
			LOG.info("params:" + params[0] + ":" + params[1]);
	}
	
	
	/**
	 * @param stateSeq
	 * @return
	 */
	private void missRate(String[] stateSeq, double[] params) {
		int start = localPredictor? 1 :  stateSeq.length - 1;
		for (int i = start; i < stateSeq.length; ++i ){
			int prState = states.indexOf(stateSeq[i -1]);
			int cuState = states.indexOf(stateSeq[i ]);
			params[0] += (cuState == maxStateProbIndex[prState]? 0 : 1);
			params[1] += 1;
		}
	}
	
	/**
	 * @param stateSeq
	 * @return
	 */
	private void entropyReduction(String[] stateSeq, double[] params) {
		int start = localPredictor? 1 :  stateSeq.length - 1;
		double entropyWoTragetState = 0;
		double entropy = 0;
		
		for (int i = start; i < stateSeq.length; ++i ){
			int prState = states.indexOf(stateSeq[i -1]);
			int cuState = states.indexOf(stateSeq[i ]);
			if (debugOn)
				LOG.info("state prob index:" + prState + " " + cuState);

			for (int j = 0; j < states.size(); ++ j) {
				double pr = stateTranstionProb[prState][j];
				double enComp = -pr * Math.log(pr);
				
				//entropy without target state
				if (j != cuState) {
					entropyWoTragetState += enComp;
				}
				
				//full entropy
				entropy += enComp;
			}
		}
		params[0] = entropyWoTragetState / entropy;
		params[1] = 1;
	}
	
}
