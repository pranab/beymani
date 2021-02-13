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
from datetime import datetime
import statistics
sys.path.append(os.path.abspath("../lib"))
from util import *
from mlutil import *
from sampler import *


# generate hourly product sales data with daily cycle and gaussian remainder
if __name__ == "__main__":
	op = sys.argv[1]

	#generate hourly sales stat which is 24 pairs of mean and std deviation
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
			print("{},{}".format(id,ms))
			
	#generate hourly sales data with sales statistics mean and std dev already generated		
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
		cTime = hourAlign(cTime)
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
				print("prodSale,{},{},{}".format(pid,sTime,total))
			sTime += sIntv

	#insert outliers in sales data for a given outlier rate	and given file with normal sales data
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
			print(mrec)
		#print count
	
	#generate abandoned shopping cart data		
	elif op == "scAb":
		interval = int(sys.argv[2])
		scDistr = GaussianRejectSampler(10,2)
		
		(cTime, pTime) = pastTime(interval, sys.argv[3])
		pTime = hourAlign(pTime)
		cTime = hourAlign(cTime)
		sTime = pTime
		sIntv = secInHour
		while sTime < cTime:
			quant = int(scDistr.sample())
			print("scAbandon,scAbandon,{},{}".format(sTime,quant))
			sTime += sIntv

	#insert outliers in abandoned shopping cart data given file with normal shopping cart data
	elif op == "olScAb":			
		fileName = sys.argv[2]
		olRate = int(sys.argv[3])
		count = 0
		for rec in fileRecGen(fileName, ","):
			if isEventSampled(olRate):
				quant = randint(15, 25)
				count += 1
				rec[3] = str(quant)
			mrec = ",".join(rec)
			print(mrec)
		print(count)
	
	##make timestamp current	
	elif op == "updTm":
		fileName = sys.argv[2]
		cTime = int(time.time())
		cTime = hourAlign(cTime)
	
		recTime = 0
		for rec in fileRecGen(fileName, ","):
			tm = int(rec[2])
			if tm > recTime:
				recTime = tm
				
		tmDiff = cTime - recTime
		for rec in fileRecGen(fileName, ","):
			tm = int(rec[2])
			tm += tmDiff
			rec[2] = str(tm)
			mrec = ",".join(rec)
			print(mrec)
			
	#generate daily sales stat for each product which is 7 pairs of mean and std deviation
	if op == "prStatWkly":
		bWeeklySeasMean = [2200,2300,2050,1800,1700,2200,2500]
		bRemStdDev = 60
		
		nProd = int(sys.argv[2])
		for i in range(nProd):
			weeklySeasStdDev = list()
			for j in range(7):
				weeklySeasStdDev.append(preturbScalar(bRemStdDev, .20))	
			weeklySeasMean = multVector(bWeeklySeasMean, .40)		
			weeklySeasMean = preturbVector(weeklySeasMean, .15)
			
			weeklySeasMeanStr = toStrList(weeklySeasMean, 3)
			weeklySeasStdDevStr = toStrList(weeklySeasStdDev, 3)
			
			msZip = list(map(lambda p: p[0] + "," + p[1], zip(weeklySeasMeanStr, weeklySeasStdDevStr)))	
			ms = ",".join(msZip)
			id = genID(10)
			print("{},{}".format(id,ms))

	#min inventory treshold and re order quantity
	elif op == "invThOrdQuan":
		statFile = sys.argv[2]

		ordPackSize = [8, 12, 16]
		for rec in fileRecGen(statFile, ","):
			offset = 0
			pid = rec[offset]
			
			offset += 1
			weeklySeasMean = rec[offset::2]
			weeklySeasMean = toFloatList(weeklySeasMean)
			
			#min inventory and order quantity 
			avSale = int(statistics.mean(weeklySeasMean))
			fuTm = randint(2,4)
			mInv = (fuTm + 1) * avSale
			orQu = randint(4,7) * avSale
			opSize = selectRandomFromList(ordPackSize)
			orQu = int(orQu / opSize) * opSize
			print("{},{},{},{}".format(pid, fuTm, mInv, orQu))

	
	#generate daily sales and inventory data with sales statistics mean and std dev already generated		
	elif op == "prSaleInvDaily":
		statFile = sys.argv[2]
		invFile = sys.argv[3]
		interval = int(sys.argv[4])
		tmUnit = sys.argv[5]
		
		#inventory
		invReord = dict()
		initInv = dict()
		for rec in fileRecGen(invFile, ","):
			pid = rec[0]
			mInv = int(rec[2])
			inv = (int(rec[1]), mInv, int(rec[3]))
			invReord[pid] = inv
			initInv[pid] = int(mInv * randomFloat(1.2, 1.8))
		

		prStat = dict()
		for rec in fileRecGen(statFile, ","):
			offset = 0
			pid = rec[offset]
			
			offset += 1
			weeklySeasMean = rec[offset::2]
			weeklySeasMean = toFloatList(weeklySeasMean)
			weeklySeasStdDev = rec[offset+1::2]
			weeklySeasStdDev = toFloatList(weeklySeasStdDev)
			
			samplers = list(map(lambda d: GaussianRejectSampler(d[0],d[1]), zip(weeklySeasMean, weeklySeasStdDev)))	
			prStat[pid] = samplers
			
		(cTime, pTime) = pastTime(interval, tmUnit)
		pTime = hourAlign(pTime)
		cTime = hourAlign(cTime)
		sTime = pTime
		sIntv = secInHour
		
		ids = prStat.keys()
		ordTm = dict()
		while sTime < cTime:
			for pid in ids:
				(fuTm, mInv, orQu) = invReord[pid]
				samplers = prStat[pid]
				dow = dayOfWeek(sTime)
				sampler = samplers[dow]
				quant = sampler.sample()
				quant = int(quant)
				
				inv = initInv[pid] - quant
				if inv < 0:
					quant += inv
					inv = 0
				initInv[pid] = inv
				if inv < mInv:
					if pid not in ordTm:
						#order
						ordTm[pid] = sTime
					else:
						eTime = int((sTime - ordTm[pid]) / secInDay)
						if eTime >= fuTm:
							#fulfilled
							initInv[pid] = initInv[pid] + orQu
							ordTm.pop(pid)
										
				print("prodSaleInv,{},{},{},{}".format(pid,sTime,quant,inv))	
						
			sTime += secInDay	
					
				
				

			
			
			
			