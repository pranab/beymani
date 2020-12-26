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
			
	if op == "agenlp":
		#abrupt drift for learner pair drift detector
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
				er1 = 1 if nsampler.sample() else 0
				if er1 == 1:
					er2 = 0 if isEventSampled(50)  else er1
				else:
					er2 = 1 if isEventSampled(10)  else er1
			else:
				er1 = 1 if osampler.sample() else 0
				er2 =  er1 ^ 1 if isEventSampled(5) else er1
			rid = genID(10)
			stime += random.randint(30, 300) 
			print("{},{},{},{}".format(rid, stime, er1, er2))
		

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
		bootstrap = len(sys.argv) >= 4 and sys.argv[3] == "true"
		evals = getFileColumnAsInt(fpath, 2, ",")
		detector = SupConceptDrift(0.9)
		if bootstrap:
			warmup = int(sys.argv[4])
			res = detector.eddm(evals, warmup)
		else:
			detector.eddmRestore("./model/eddm.mod")
			res = detector.eddm(evals)
		detector.eddmSave("./model/eddm.mod")
		
		for r in res:
			print("{:.3f},{:.3f},{:.3f},{}".format(r[0],r[1],r[2],r[3]))
		dr = list(map(lambda v: v[3], res))
		drawLine(dr)

	elif op == "lp":
		#DDM detector
		fpath = sys.argv[2]
		bootstrap = len(sys.argv) >= 4 and sys.argv[3] == "true"
		evals = getFileAsIntMatrix(fpath, [2,3])
		detector = SupConceptDrift(0.5)
		if bootstrap:
			warmup = int(sys.argv[4])
			res = detector.lp(evals, warmup)
		else:
			detector.lpRestore("./model/lp.mod")
			res = detector.lp(evals)
		detector.lpSave("./model/lp.mod")
		
	elif op == "ecdd":
		#ECDD detector
		fpath = sys.argv[2]
		threshold = float(sys.argv[3])
		bootstrap = len(sys.argv) >= 5 and sys.argv[4] == "true"
		evals = getFileColumnAsInt(fpath, 2, ",")
		detector = SupConceptDrift(threshold)
		if bootstrap:
			expf = float(sys.argv[5])
			warmup = int(sys.argv[6])
			res = detector.ecdd(evals, expf, warmup)
		else:
			detector.ecddRestore("./model/ecdd.mod")
			res = detector.ecdd(evals, 0.0)
		detector.ecddSave("./model/ecdd.mod")
		
		for r in res:
			print("{},{:.3f},{}".format(r[0],r[1],r[2]))
		dr = list(map(lambda v: v[2], res))
		drawLine(dr)

	else:
		exitWithMsg("invalid command")	