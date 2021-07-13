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
from sklearn.neighbors import KDTree
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
sys.path.append(os.path.abspath("../supv"))
from util import *
from sampler import *
from sucodr import *
from knn import *

"""
concept drift data generation 
"""

def linTrans(val, scale, shift):
	return val * scale + shift
	
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

	elif op == "genrc":
		#generate retail churn data
		numSample = int(sys.argv[2])
		noise = float(sys.argv[3])
		
		cids = None
		if len(sys.argv) == 5:
			rfPath = sys.argv[4]
			cids = getFileColumnAsString(rfPath, 0)
		
		classes = ["1", "0"]
		chDistr = CategoricalRejectSampler(("1", 30), ("0", 70))
		featCondDister = {}
		
		#trans amount
		fi = 0
		key = ("1", fi)
		distr = NormalSampler(50, 10)
		featCondDister[key] = distr
		key = ("0", fi)
		distr = NormalSampler(150, 15)
		featCondDister[key] = distr
		
		#average num days gap
		fi += 1
		key = ("1", fi)
		distr = NormalSampler(30, 10)
		featCondDister[key] = distr
		key = ("0", fi)
		distr = NormalSampler(15, 5)
		featCondDister[key] = distr
		
		#avearge visit duration sec
		fi += 1
		key = ("1", fi)
		distr = NormalSampler(600, 120)
		featCondDister[key] = distr
		key = ("0", fi)
		distr = NormalSampler(1200, 180)
		featCondDister[key] = distr
		
		#avearge num searches
		fi += 1
		key = ("1", fi)
		distr = DiscreteRejectSampler(1, 3, 1, 100, 50, 20)
		featCondDister[key] = distr
		key = ("0", fi)
		distr = DiscreteRejectSampler(1, 4, 1, 70, 100, 80, 50)
		featCondDister[key] = distr
		
		#avearge num of customer issues
		fi += 1
		key = ("1",fi)
		distr = DiscreteRejectSampler(1, 6, 1, 60, 80, 100, 90, 70, 40)
		featCondDister[key] = distr
		key = ("0", fi)
		distr = DiscreteRejectSampler(1, 2, 1, 100, 40)
		featCondDister[key] = distr
		
		#avearge num calls for resolution
		fi += 1
		key = ("1", fi)
		distr = DiscreteRejectSampler(1, 3, 1, 40, 100, 20)
		featCondDister[key] = distr
		key = ("0", fi)
		distr = DiscreteRejectSampler(1, 2, 1, 100, 50)
		featCondDister[key] = distr

		#avearge num payment problems
		fi += 1
		key = ("1", fi)
		distr = DiscreteRejectSampler(1, 3, 1, 40, 60, 100)
		featCondDister[key] = distr
		key = ("0", fi)
		distr = DiscreteRejectSampler(0, 1, 1, 100, 70)
		featCondDister[key] = distr
		
		sampler = AncestralSampler(chDistr, featCondDister, 7)
		for i in range(numSample):
			(claz, features) = sampler.sample()
			features[1] = int(features[1])
			features[2] = int(features[2])
			claz = addNoiseCat(claz, classes, noise)
			cid = genID(10) if cids is None else cids[i]
			rec = cid + "," + toStrFromList(features, 2) + "," + claz
			print(rec)

	elif op == "recl":
		#replace class label
		fpath = sys.argv[2]
		cl = sys.argv[3]
		for rec in fileRecGen(fpath, ","):
			le = len(rec)
			rec[le - 1] = cl
			print(",".join(rec))

	elif op == "dish":
		#add distr shift
		fpath = sys.argv[2]
		for rec in fileRecGen(fpath, ","):
			fi = 1
			tran = linTrans(float(rec[fi]), 1.1, 30)
			rec[fi] = tran
			fi += 1
			ga = int(linTrans(int(rec[fi]), 0.95, -6))
			rec[fi] = ga
			fi += 1
			du = int(linTrans(int(rec[fi]), 1.2, 120))
			rec[fi] = du
			fi += 1
			srch = int(linTrans(int(rec[fi]), 1.3, 2))
			rec[fi] = srch
			fi += 1
			issue = int(linTrans(int(rec[fi]), 1, 1))
			rec[fi] = issue
			fi += 2
			pissue = int(linTrans(int(rec[fi]), 1, 1))
			rec[fi] = pissue
			r = toStrFromList(rec, 2)
			print(r)
		
	elif op == "ludrift":
		#add distr shift
		fPath = sys.argv[2]
		selFieldIndices = [1,2,3,4,5,6,7,8]
		featFieldIndices = [0,1,2,3,4,5,6]
		(aData, fData) = loadDataFile(fPath, ",", selFieldIndices, featFieldIndices)
		l = len(fData)
		lh = int(l/2)
		nSize = float(sys.argv[3])
		
		if len(sys.argv) == 5:
			print("with drift case")
			ai = int(l * randomFloat(.48, .52))	
			aData = fData[ai]
			nCount = int(float(sys.argv[4]) * l)
			tree = KDTree(fData, leaf_size=2)
		
			nDist, nInd = tree.query([aData], k=nCount)
			neighbors = nInd[0]
		else:
			neighbors = None
			print("no drift case")
		
		for i in range(l):
			rec = fData[i]
			if neighbors is not None and  i >= lh and i in neighbors:
				fi = 0
				tran = linTrans(rec[fi], 1.1, 30)
				rec[fi] = tran
				fi += 1
				ga = int(linTrans(rec[fi], 0.95, -6))
				rec[fi] = float(ga)
				fi += 1
				du = int(linTrans(rec[fi], 1.2, 120))
				rec[fi] = float(du)
				fi += 1
				srch = int(linTrans(rec[fi], 1.3, 2))
				rec[fi] = float(srch)
				fi += 1
				issue = int(linTrans(rec[fi], 1, 1))
				rec[fi] = float(issue)
				fi += 2
				pissue = int(linTrans(rec[fi], 1, 1))
				rec[fi] = float(pissue)
		
		dd = UnsupConceptDrift()
		dd.localDrift(fData, nSize, 50)
		
	elif op == "udrift":
		#unsupervised drift detection with knn classifier
		cfPath = sys.argv[2]
		knnClass = NearestNeighbor(cfPath)
		knnClass.train()
		cres = knnClass.predictProb()
		prl = list()
		for r in cres:
			pr = r[0] if r[0] > r[1] else r[1]
			prl.append(pr)
			print(r)
		mpr = statistics.mean(prl)	
		print("mean prediction probability {:.3f}".format(mpr))
		
	else:
		exitWithMsg("invalid command")	