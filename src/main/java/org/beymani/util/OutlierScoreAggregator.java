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

import java.io.Serializable;
import java.util.Arrays;

import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class OutlierScoreAggregator implements Serializable{
	private int size;
	private double[] weights;
	private double[] scores;
	private boolean[] available;
	private int indx = 0; 
	
	/**
	 * @param size
	 * @param weights
	 */
	public OutlierScoreAggregator(int size, double[] weights) {
		this.weights = weights;
		this.size = size;
		scores = new double[size];
		available = new boolean[size];
	}

	/**
	 * 
	 */
	public void initialize() {
		indx = 0;
	}
	
	/**
	 * @param score
	 */
	public void addScore(double score) {
		scores[indx] = score;
		available[indx++] = true;
	}
	
	/**
	 * 
	 */
	public void addScore() {
		available[indx++] = false;
	}
	
	/**
	 * @return
	 */
	public double getAverage() {
		BasicUtils.assertCondition(indx == size, "all scores not collected indx " + indx + " size " + size);
		double sum = 0;
		int count = 0;
		for (int i = 0; i < size; ++i) {
			if (available[i]) {
				sum += scores[i];
				++count ;
			}
		}
		
		return sum/count;
	}

	/**
	 * @return
	 */
	public double getWeightedAverage() {
		BasicUtils.assertCondition(indx == size, "all scores not collected indx " + indx + " size " + size);
		double sum = 0;
		double totalWeight = 0;
		for (int i = 0; i < size; ++i) {
			if (available[i]) {
				sum += scores[i] * weights[i];
				totalWeight += weights[i];
			}
		}
		return sum/totalWeight;
	}
	
	/**
	 * @return
	 */
	public double getMedian() {
		BasicUtils.assertCondition(indx == size, "all scores not collected indx " + indx + " size " + size);
		double median = 0;
		int avCount = 0;
		for (boolean av : available) {
			if (av) {
				++avCount;
			}
		}
		double[] validScores = new double[avCount];
		for (int i = 0, j = 0; i < size; ++i){
			if (available[i]) {
				validScores[j++] = scores[i];
			}
		}
		Arrays.sort(validScores);
		if (avCount % 2 == 1) {
			median = validScores[avCount/2];
		} else {
			median = (validScores[avCount/2 - 1] + validScores[avCount/2]) / 2;
		}
		return median;
	}
	
}
