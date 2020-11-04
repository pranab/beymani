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
from random import randint
import time
import uuid
import threading
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

def createAnomaly(high):
	if high:
		reading = randomFloat(120, 200)
	else:
		reading = randomFloat(60, 80)
	return reading
	
if __name__ == "__main__":
	op = sys.argv[1]
	
	#device stats
	if op == "stat":
		#normal mean 80 - 100 sd 1 - 5 
		#anomaly  mean 120 - 160 sd 1 - 5 
		numDevs = int(sys.argv[2])
		mmin = int(sys.argv[3])
		mmax = int(sys.argv[4])
		smin = int(sys.argv[5])
		smax = int(sys.argv[6])
		for i in range(numDevs):
			mean = randomFloat(mmin, mmax)
			sd = randomFloat(smin, smax)
			devId = genID(12)
			#print "%s,%.3f,%.3f" %(devId, mean, sd)
			print("{},{:.3f},{:.3f}".format(devId, mean, sd))
			
	#generate reading		
	elif op == "gen":
		statFile = sys.argv[2]
		numDays = int(sys.argv[3])
		modeNorm = (sys.argv[4] == "normal")
		
		devices = []
		for rec in fileRecGen(statFile, ","):
			ds = (rec[0], float(rec[1]), float(rec[2]))
			devices.append(ds)
			
		
		numDevs = len(devices)
		distrs = list(map(lambda d: GaussianRejectSampler(d[1],d[2]), devices))	

		curTime = int(time.time())
		pastTime = curTime - (numDays + 1) * secInDay
		pastTime = (pastTime / secInDay) * secInDay + secInHour * 15
		sampTime = pastTime
		sampIntv = secInDay
		
		anm = dict()
		anmDesc = dict()
		while(sampTime < curTime):
			for i in range(numDevs):
				d = devices[i]
				did = d[0]
				ts = sampTime + randint(-1000, 1000)
				sampled = False
				anomalyRate = 10 if (modeNorm) else 20
				if isEventSampled(anomalyRate):
					if not did in anm:
						#create anomaly
						high = isEventSampled(80)
						reading =  createAnomaly(high)
						appendKeyedList(anm, did, reading)
						length = randint(1, 2) if(modeNorm) else randint(3, 7)
						desc = (length, high)
						anmDesc[did] = desc
						sampled = True
						#print "**** anomaly created %s, %d" %(did, reading)
				
				if not sampled:
					if did in anm:
						# ongoing anomaly
						ans = anm[did]
						desc = anmDesc[did]
						towardsNorm = len(ans) == desc[0] 
						an = ans[0]
						if len(ans) == desc[0]:
							# moving toward normal from anomaly
							if isEventSampled(60):
								sampled = True
								reading = 0.85 * an if(desc[1]) else 1.15 * an
								#print "**** moving back to normal %s, %d" %(did, reading)
							del anm[did]
							del anmDesc[did]
						elif len(ans) < desc[0]:
							# continue anomaly
							reading = createAnomaly(desc[1])
							appendKeyedList(anm, did, reading)
							sampled = True
							#print "**** anomaly continued %s, %d" %(did, reading)
						
					if not sampled:
						# normal
						reading = distrs[i].sample()
				
				#print "%s,%d,%d" %(did, ts, int(reading))
				print("{},{},{}".format(did, ts, int(reading)))
			sampTime += sampIntv 

	elif op == "oplot":
		#plot outliers
		fpath = sys.argv[2]
		mid = sys.argv[3]
		filt = lambda r : r[0] == mid
		dvalues = list(map(lambda r : float(r[3]), fileFiltRecGen(fpath, filt)))
		xvalues = list(map(lambda r : int(r[1]), fileFiltRecGen(fpath, filt)))
		plt.plot(xvalues, dvalues)
		plt.title("outlier score")
		plt.show()
				