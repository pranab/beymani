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

def olScoreStat(dirPath, keyLen):
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
		sim.drawHist("outlier score", "score", "freq")
		stats = sim.getUpperTailStat(0)
		print("key: {}".format(kstr))
		for s in stats:
			print("{:.3f} {:.3f}".format(s[0], s[1]))
		
if __name__ == "__main__":
	dirPath = sys.argv[1]
	keyLen = int(sys.argv[2])
	olScoreStat(dirPath, keyLen)
			
				

