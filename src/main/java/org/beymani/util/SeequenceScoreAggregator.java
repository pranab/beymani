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

package org.beymani.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages  outlier scores for data points in a sequence. A data point may belong to 
 * multiple sequences and hence may have have multiple outlier scores
 * @author pranab
 *
 */
public class SeequenceScoreAggregator implements java.io.Serializable {
	private static final long serialVersionUID = 2181114339589177954L;
	private List<Double> scores = new ArrayList<Double>();
	private int windowSize;
	
	
	/**
	 * @param windowSize
	 */
	public SeequenceScoreAggregator(int windowSize) {
		super();
		this.windowSize = windowSize;
	}
	
	
	/**
	 * @param seq
	 * @param score
	 */
	public void add(double score ) {
		scores.add(score);
		if (scores.size() > windowSize) {
			//set score to max of current and new score
			for (int i = scores.size() - windowSize; i < scores.size(); ++i) {
				double thisSeqScore = scores.get(i);
				if (thisSeqScore < score) {
					scores.set(i, score);
				}
			}
		}
	}

	/**
	 * @return
	 */
	public List<Double> getScores() {
		return scores;
	}
	
}
