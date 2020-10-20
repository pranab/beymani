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

import org.chombo.util.Pair;

/**
 * Outlier score for a sequence element
 * @author pranab
 *
 */
public class SequencedScore extends Pair<Long, Double> {
	private static final long serialVersionUID = 4277362152194891790L;

	public SequencedScore(long seq, double score) {
		super(seq, score);
	}
	
	public long getSeq() {
		return left;
	}

	public double getScore() {
		return right;
	}

	public void setScore(double score) {
		right = score;
	}
}
