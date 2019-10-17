#!/usr/bin/python

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
from datetime import datetime
sys.path.append(os.path.abspath("../lib"))
from util import *
from mlutil import *
from sampler import *


# generate hourly product sales data with daily cycle and gaussian remainder
if __name__ == "__main__":
	op = sys.argv[1]

	#generate sales stat
	if op == "prStat":
		bDailySeasMean = [91,85,67,42,37,25,43,79,102,140,173,288,404,486,388,334,381,427,319,423,563,442,261,167]
		bRemStdDev = 10
		
		nProd = int(sys.argv[2])
		for i in range(nProd):
			dailySeasStdDev = list()
			for j in range(24):
				dailySeasStdDev.append(preturbScalar(bRemStdDev, .20))	
			dailySeasMean = multVector(bDailySeasMean, .40)		
			dailySeasMean = preturbVector(dailySeasMean, .15)
			
			dailySeasMeanStr = toStrList(dailySeasMean, 3)
			dailySeasStdDevStr = toStrList(dailySeasStdDev, 3)
			
			msZip = list(map(lambda p: p[0] + "," + p[1], zip(dailySeasMeanStr, dailySeasStdDevStr)))	
			ms = ",".join(msZip)
			id = genID(10)
			print "%s,%s" %(id,ms)
			
	#generate sales data		
	elif op == "prSale":
		statFile = sys.argv[2]
		interval = int(sys.argv[3])

		prStat = dict()
		for rec in fileRecGen(statFile, ","):
			offset = 0
			pid = rec[offset]
			
			offset += 1
			dailySeasMean = rec[offset::2]
			dailySeasMean = toFloatList(dailySeasMean)
			dailySeasStdDev = rec[offset+1::2]
			dailySeasStdDev = toFloatList(dailySeasStdDev)
			
			samplers = list(map(lambda d: GaussianRejectSampler(d[0],d[1]), zip(dailySeasMean, dailySeasStdDev)))	
			prStat[pid] = samplers
		
		(cTime, pTime) = pastTime(interval, sys.argv[4])
		pTime = hourAlign(pTime)
		sTime = pTime
		sIntv = secInHour
		#print pTime, cTime
		
		ids = prStat.keys()
		while sTime < cTime:
			for pid in ids:
				samplers = prStat[pid]
				hod = hourOfDay(sTime)
				sampler = samplers[hod]
				quant = sampler.sample()
				total = int(quant)
				total =  minLimit(total, 0)
				print "prodSale,%s,%d,%d" %(pid,sTime,total)  
			sTime += sIntv

	#insert outliers in sales data		
	elif op == "olPrSale":			
		fileName = sys.argv[2]
		olRate = int(sys.argv[3])
		count = 0
		for rec in fileRecGen(fileName, ","):
			if isEventSampled(olRate):
				quant = int(rec[3])
				if (isEventSampled(30)):
					quant += 200
				else:
					quant -= 300
					if (quant < 0):
						quant = 0
				count += 1
				rec[3] = str(quant)
			mrec = ",".join(rec)
			print mrec
		#print count
			
