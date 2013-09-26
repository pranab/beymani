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
	private Map<String, List<String>> stateSequences = new HashMap<String, List<String>>(); 
	private boolean globalPredictor;
	private boolean localPredictor;
	private int stateSeqWindowSize;
	
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
		        Utility.deseralizeTableRow(stateTranstionProb, line, ",", row, numStates);
		        ++row;
		  }
		  ++lineCount;
		}

		 scanner.close();
		 globalPredictor = Boolean.parseBoolean(conf.get("global.predictor").toString());
		 localPredictor = Boolean.parseBoolean(conf.get("local.predictor").toString());
		 if (localPredictor) {
			 stateSeqWindowSize =  Integer.parseInt(conf.get("state.seq.window.size").toString());
		 }
	}

	@Override
	public double execute(String entityID, String record) {
		List<String> recordPair = records.get(entityID);
		if (null == recordPair) {
			recordPair = new ArrayList<String>();
		}
		recordPair.add(record);
		if (recordPair.size() > 2) {
			recordPair.remove(0);
		}
		
		String state = null;
		if (recordPair.size() == 2) {
			//get state
		}
		
		//state sequence
		List<String> stateSeq = stateSequences.get(entityID);
		if (null == stateSeq) {
			stateSeq = new ArrayList<String>();
		}
		stateSeq.add(state);
		if (stateSeq.size() > stateSeqWindowSize) {
			stateSeq.remove(0);
		}
		
		double score = getScore( stateSeq);
		return score;
	}
	
	/**
	 * @param stateSeq
	 * @return
	 */
	private double getScore(List<String> stateSeq) {
		boolean first  = true;
		int curIndex = 0, preIndex = 0;
		double condPrDist = 0;
		for (String state : stateSeq) {
			if (first) {
				curIndex = states.indexOf(state);
				first = false;
			} else {
				preIndex = curIndex;
				curIndex = states.indexOf(state);
				condPrDist += Math.log(stateTranstionProb[preIndex][curIndex]);
			}
		}
		return condPrDist; 
	}

}
