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

# Package imports
import os
import sys
import random
import statistics 
import matplotlib.pyplot as plt 
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
sys.path.append(os.path.abspath("../unsupv"))
from util import *
from sampler import *
from ae import *

"""
data generation for service ticket 
"""


if __name__ == "__main__":
	op = sys.argv[1]
	if op == "gen":
		"""
		data generation for service ticket consisting ticket ID, severity, prority, service time in hour
		"""
		numSamp = int(sys.argv[2])
		
		scIds = ["JDG34JHD3H", "TK80SDH6N2"]
		
		#service time in hour distribution based on severity and priority
		distr = dict()
		sePr = (1,1)
		distr[sePr] = NonParamRejectSampler(2, 8, 80, 100, 50, 10, 5)
		sePr = (2,1)
		distr[sePr] = NonParamRejectSampler(2, 8, 30, 60, 80, 100, 60, 40, 10)
		sePr = (3,1)
		distr[sePr] = NonParamRejectSampler(2, 8, 10, 40, 60, 80, 90, 100, 95, 80, 60, 40, 25, 5)
		
		sePr = (1,2)
		distr[sePr] = NonParamRejectSampler(2, 8, 100, 70, 40, 10, 5)
		sePr = (2,2)
		distr[sePr] = NonParamRejectSampler(2, 8, 45, 70, 100, 60, 40, 25, 5)
		sePr = (3,2)
		distr[sePr] = NonParamRejectSampler(2, 8, 30, 60, 75, 90, 100, 90, 80, 70, 50, 25, 10)
		
		sevDistr = DiscreteRejectSampler(1, 3, 1, 70, 100, 40)
		priDistr = DiscreteRejectSampler(1, 2, 1, 100, 30)
		
		tids = [100001, 500001]
		for _ in range(numSamp):
			sci = random.randint(0, 1)
			tid = tids[sci] 
			sev = sevDistr.sample()
			pri = priDistr.sample()
			#print(sev, pri)
			x = (sev, pri)
			stime = int(distr[x].sample())
			if sci == 1:
				#second service center lakes longer
				stime = int(1.2 * stime + 4)
			print("{},{},{},{},{}".format(scIds[sci],tid, sev, pri, stime))
			tids[sci] += 1

	elif op == "iol":
		filePath = sys.argv[2]
		olRate = int(sys.argv[3])
		for rec in fileRecGen(filePath, ","):
			if isEventSampled(olRate):
				svcTm = int(rec[4])
				osvcTm = svcTm + random.randint(30, 50)
				if osvcTm > 3 * svcTm:
					osvcTm = 3 * svcTm
				rec[4] = str(osvcTm)
			mrec = ",".join(rec)
			print(mrec)
			
	elif op == "genx":
		"""
		data generation for service ticket consisting ticket ID, severity, prority, num of calls, 
		num of messages and emails, customer sentiment, reopened or not
		"""
		numSamp = int(sys.argv[2])
		
		#service time distribution in days based on severity and priority
		tmDistr = dict()
		sePr = (1,1)
		tmDistr[sePr] = NonParamRejectSampler(1, 1, 80, 100, 50, 10, 5)
		sePr = (2,1)
		tmDistr[sePr] = NonParamRejectSampler(1, 1, 30, 60, 80, 100, 60, 40, 10)
		sePr = (3,1)
		tmDistr[sePr] = NonParamRejectSampler(1, 1, 10, 40, 60, 80, 90, 100, 95, 80, 60, 40, 25, 5)
		
		sePr = (1,2)
		tmDistr[sePr] = NonParamRejectSampler(1, 1, 100, 70, 40, 10, 5)
		sePr = (2,2)
		tmDistr[sePr] = NonParamRejectSampler(1, 1, 45, 70, 100, 60, 40, 25, 5)
		sePr = (3,2)
		tmDistr[sePr] = NonParamRejectSampler(1, 1, 30, 60, 75, 90, 100, 90, 80, 70, 50, 25, 10)
		
		sevDistr = DiscreteRejectSampler(1, 3, 1, 70, 100, 40)
		priDistr = DiscreteRejectSampler(1, 2, 1, 100, 30)
		
		#num of call distribution based on severity
		ncallDistr = dict()
		ncallDistr[1] = NormalSampler(1.2, 0.5)
		ncallDistr[2] = NormalSampler(2.5, 1.4)
		ncallDistr[3] = NormalSampler(4.5, 2.0)

		#num of call distribution based on severity
		nmsgDistr = dict()
		nmsgDistr[1] = NormalSampler(0.7, 0.5)
		nmsgDistr[2] = NormalSampler(1.2, 0.9)
		nmsgDistr[3] = NormalSampler(2.2, 1.4)
		
		#sentiment
		sentDistr = dict()
		sentDistr[1] = NonParamRejectSampler(-1.0, .5, 20, 35, 100, 60, 25)
		sentDistr[2] = NonParamRejectSampler(-1.0, .5, 30, 50, 100, 40, 15)
		sentDistr[3] = NonParamRejectSampler(-1.0, .5, 40, 70, 100, 30, 10)
		for s in sentDistr.values():
			s.sampleAsFloat()

		#reopen
		reopenDistr = dict()
		reopenDistr[1] = BernoulliTrialSampler(0.10)
		reopenDistr[2] = BernoulliTrialSampler(0.20)
		reopenDistr[3] = BernoulliTrialSampler(0.35)

		for _ in range(numSamp):
			tid = genID(10) 
			sev = sevDistr.sample()
			pri = priDistr.sample()
			x = (sev, pri)
			stime = int(tmDistr[x].sample())
			ncall = int(ncallDistr[sev].sample())
			ncall = minLimit(ncall, 1)
			nmsg = int(nmsgDistr[sev].sample())
			nmsg = minLimit(nmsg, 0)
			sent = sentDistr[sev].sample()
			reopen =  1 if reopenDistr[sev].sample() else 0
			if reopen == 1:
				stime = int(0.9 * stime)
				ncall -= 1
				ncall = minLimit(ncall, 1)
				nmsg -= 1
				nmsg = minLimit(nmsg, 0)
			stime = minLimit(stime, 1)	
			print("{},{},{},{},{},{:.2f},{},{}".format(tid, sev, pri, ncall, nmsg, sent, reopen, stime))
			

	elif op == "iolx":
		filePath = sys.argv[2]
		olRate = int(sys.argv[3])
		atypeDistr = DiscreteRejectSampler(1, 3, 1, 100, 40, 30)
		for rec in fileRecGen(filePath, ","):
			if isEventSampled(olRate):
				atype = atypeDistr.sample()
				if atype == 1:
					#service time
					svcTm = int(rec[7])
					osvcTm = svcTm + random.randint(1, 5)
					rec[7] = str(osvcTm)
				elif atype == 2:
					#num of calls
					ncall = int(rec[3])
					ncall += random.randint(2, 5)
					rec[3] = str(ncall)
				elif atype == 3:
					#sentiment
					sent = float(rec[5])
					sent -= randomFloat(.2, .5)
					sent = minLimit(sent, -1.0)
					rec[5] = "{:.2f}".format(sent)
				
			mrec = ",".join(rec)
			print(mrec)
							 		
	elif op == "train":
		prFile = sys.argv[2]
		auenc = AutoEncoder(prFile)
		auenc.buildModel()
		auenc.trainModel()
		
	elif op == "regen":
		prFile = sys.argv[2]
		auenc = AutoEncoder(prFile)
		auenc.buildModel()
		scores = auenc.regen()
		plt.hist(scores, bins=30, cumulative=False, density=False)
		plt.show()
		print(sorted(scores, reverse=True)[:20])

	else:
		exitWithMsg("invalid command")	
		
