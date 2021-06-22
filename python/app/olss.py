#!/usr/local/bin/python3

# avenir-python: Machine Learning
# Author: Pranab Ghosh
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You may
# obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

import os
import sys
import time
import math
import statistics 
import ntpath
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
from util import *
from mlutil import *
from mcsim import *

"""
Statistical test for outlier score to determine suitable score threshold
"""

def getKeyedOlScores(dirPath, keyLen):
	'''
	extracts outlier score from spark output files
	'''
	filePaths = getAllFiles(dirPath) 
	scores = dict()
	if keyLen == 0:
		kstr = "all"
	for fpath in filePaths:
		fname = ntpath.basename(fpath)
		if fname.startswith("part"):
			print("processing {}".format(fpath))
			for rec in fileRecGen(fpath, ","):
				if keyLen > 0:
					kstr = ",".join(rec[0:keyLen])
				score = float(rec[-2])
				vl = scores.get(kstr)
				if vl is None:
					vl = list()
					scores[kstr] = vl					
				vl.append(score)
	return scores

def olScoreStat(dirPath, keyLen, shoHist):
	"""
	upper tail statistic for outlier score
	"""
	filePaths = getAllFiles(dirPath) 
	scores = dict()
	if keyLen == 0:
		kstr = "all"
	for fpath in filePaths:
		fname = ntpath.basename(fpath)
		if fname.startswith("part"):
			print("processing {}".format(fpath))
			for rec in fileRecGen(fpath, ","):
				if keyLen > 0:
					kstr = ",".join(rec[0:keyLen])
				score = float(rec[-2])
				vl = scores.get(kstr)
				if vl is None:
					vl = list()
					scores[kstr] = vl					
				vl.append(score)
	
	print("outlier score upper tail stats")
	sim = MonteCarloSimulator(None,None,None,None)	
	for kstr, vl in scores.items():			
		sim.setOutput(vl)
		if shoHist:
			sim.drawHist("outlier score", "score", "freq")
		stats = sim.getUpperTailStat(0)
		print("key: {}".format(kstr))
		for s in stats:
			print("{:.3f} {:.3f}".format(s[0], s[1]))

def olScoreEvStat(dirPath, keyLen, prTh, exPrTh):
	"""
	extreme value statistic for outlier score
	Paper: Anomaly Detection in Streams with Extreme Value Theory by Siffer, 
	"""
	scores = getKeyedOlScores(dirPath, keyLen)
	
	sim = MonteCarloSimulator(None,None,None,None)	
	for kstr, vl in scores.items():			
		sim.setOutput(vl)
		vth = sim.getCritValue(self, prTh)
		
		#values above threshold
		y = list(filter(lambda v : v > vth, vl))	
		ymax = max(y)
		ymin = min(y)
		ymean = statistics.mean(y)
		xsmin = -1.0 / ymax
		xsmax = 2.0 * (ymean - ymin) / (ymean * ymean)
		delta = (xsmax - xsmin) / 100
		for xs in floatRange(xsmin, xsmax, delta):
			pass
			
		
		
if __name__ == "__main__":
	technique = sys.argv[1]
	dirPath = sys.argv[2]
	keyLen = int(sys.argv[3])
	
	if technique == "sttest":
		"""  outlier score upper tail statistics """
		shoHist = sys.argv[4] == "hist" if len(sys.argv) == 5 else False
		olScoreStat(dirPath, keyLen, shoHist)
		
	elif technique == "exvstat":
		"""  extreme value statistic for outlier score  """
		prTh = float(sys.argv[4])
		exPrTh = float(sys.argv[5])
		olScoreEvStat(dirPath, keyLen, prTh, exPrTh)
	else:
		exitWithMsg("invalid technique")
	
			
				

