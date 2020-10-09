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
from util import *
from sampler import *

"""
data generation for service ticket consisting ticket ID, severity, prority, service time in hour
"""
if __name__ == "__main__":
	op = sys.argv[1]
	if op == "gen":
		numSamp = int(sys.argv[2])
		
		scIds = ["JDG34JHD3H", "TK80SDH6N2"]
		
		#service time distribution based on severity and priority
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
			
				 		
		
		
		
