#!/usr/local/bin/python3

# beymani-python: Machine Learning
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

# Package imports
import os
import sys
import random
import statistics 
import matplotlib.pyplot as plt 
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
from util import *
from sampler import *
from sucodr import *

"""
concept drift data generation nd driver
"""

if __name__ == "__main__":
	op = sys.argv[1]
	if op == "agen":
		#abrupt drift
		nsamp = int(sys.argv[2])
		oerate = float(sys.argv[3])
		osampler = BernoulliTrialSampler(oerate)
		trans = None
		if len(sys.argv) == 6:
			trans = int(float(sys.argv[4]) * nsamp)
			nerate = float(sys.argv[5])
			nsampler = BernoulliTrialSampler(nerate)
		curTime, pastTime = pastTime(10, "d")
		stime = pastTime
		for i in range(nsamp):
			if trans is not None and i > trans:
				er = 1 if nsampler.sample() else 0
			else:
				er = 1 if osampler.sample() else 0
			rid = genID(10)
			stime += random.randint(30, 300) 
			print("{},{},{}".format(rid, stime, er))
			
	if op == "plot":
		fpath = sys.argv[2]
		evals = getFileColumnAsInt(fpath, 2, ",")
		drawLine(evals)
		
	elif op == "ddm":
		#DDM detector
		fpath = sys.argv[2]
		evals = getFileColumnAsInt(fpath, 2, ",")
		detector = SupConceptDrift(3.5)
		res = detector.ddm(evals, 30)
		for r in res:
			print("{:.3f},{:.3f},{:.3f},{}".format(r[0],r[1],r[2],r[3]))
		dr = list(map(lambda v: v[3], res))
		drawLine(dr)
		
	elif op == "eddm":
		#DDM detector
		fpath = sys.argv[2]
		bootstrap = len(sys.argv) == 4 and sys.argv[3] == "true"
		evals = getFileColumnAsInt(fpath, 2, ",")
		detector = SupConceptDrift(0.9)
		if bootstrap:
			res = detector.eddm(evals, 700)
		else:
			detector.eddmRestore("./model/eddm.mod")
			res = detector.eddm(evals)
		detector.eddmSave("./model/eddm.mod")
		
		for r in res:
			print("{:.3f},{:.3f},{:.3f},{}".format(r[0],r[1],r[2],r[3]))
		dr = list(map(lambda v: v[3], res))
		drawLine(dr)
		