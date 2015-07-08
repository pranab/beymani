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
 * Various sequence matching algorithms
 * @author pranab
 *
 * @param <T>
 */
public class Sequence<T> {
	private List<T> seqData = new ArrayList<T>();
	private int maxSize;
	private int sim;

	public Sequence() {
	}
	
	public Sequence(int maxSize) {
		this.maxSize = maxSize;
	}
	
	public void add(T item) {
		seqData.add(item);
		if (maxSize > 0 && seqData.size() > maxSize) {
			seqData.remove(0);
		}
	}
	
	/**
	 * Simple positional matching
	 * @param other
	 * @return
	 */
	public int matchCount(Sequence<T> other) {
		int matchSize = seqData.size() < other.seqData.size() ? seqData.size() : other.seqData.size();
		sim = 0;
		for (int i = 0; i < matchSize; ++i) {
			if (seqData.get(i).equals(other.seqData.get(i))) {
				++sim;
			}
		}
		return sim;
	}
	
	/**
	 * Positional matching with higher reward for adjacent mactches
	 * @param other
	 * @return
	 */
	public int adjacencyRewardedMatchCount(Sequence<T> other) {
		int matchSize = seqData.size() < other.seqData.size() ? seqData.size() : other.seqData.size();
		sim = 0;
		int adjCount = 1;
		for (int i = 0; i < matchSize; ++i) {
			if (seqData.get(i).equals(other.seqData.get(i))) {
				sim += adjCount;
				++adjCount;
			} else {
				adjCount = 1;
			}
		}		
		return sim;
	}	
	
	/**
	 * Positional matching with higher reward for adjacent mactches
	 * @param other
	 * @return
	 */
	public int maxCommonSubSeqMatchCount(Sequence<T> other) {
		int matchSize = seqData.size() < other.seqData.size() ? seqData.size() : other.seqData.size();
		sim = 0;
		int adjCount = 0;
		for (int i = 0; i < matchSize; ++i) {
			if (seqData.get(i).equals(other.seqData.get(i))) {
				++adjCount;
			} else {
				if (adjCount > sim) {
					sim = adjCount;
				}
				adjCount = 0;
			}
		}		
		return sim;
	}	
	
}
