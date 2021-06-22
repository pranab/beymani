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


def tmSampleInSec(sampler):
	"""
	sampled time in sec
	"""
	return int(sampler.sample() * 60)
	
def gaussianDistOutlier(mean, sd, polarity="all"):
	"""
	outlier as per gaussian distr
	"""
	z = randomFloat(3, 4)
	if polarity == "neg":
		z = -z
	elif polarity == "all":
		if isEventSampled(50):
			z = -z
	s = 60 * (mean + z * sd)
	return int(s)
	
def outlierTimeElapsed(st, ex, ncat):
	"""
	anomalaous elspased time for state
	"""
	if st == 1:
		#ordPlaceDist
		te = gaussianDistOutlier(8, 2, "pos")
	elif st == 2:
		#frCheckDist
		te = gaussianDistOutlier(1, .3, "pos")
	elif st == 3:
		te = gaussianDistOutlier(0, 0, "pos")
	elif st == 4 or st == 5:
		#manApprovDist
		te = gaussianDistOutlier(60, 12, "pos") if ex == 1 else gaussianDistOutlier(30, 8, "pos")
	elif st == 6:
		#warehouseConfirmDist
		te = gaussianDistOutlier(10, 1, "pos")
	elif st == 7:
		#pickDist
		if ncat == 1:
			te = gaussianDistOutlier(10, 1, "pos")
		elif ncat == 2:
			te = gaussianDistOutlier(18, 1.2, "pos")
		elif ncat == 3:
			te = gaussianDistOutlier(15, 1.4, "pos")
		elif ncat == 4:
			te = gaussianDistOutlier(21, 1.7, "pos")
		elif ncat == 5:
			te = gaussianDistOutlier(26, 2.1, "pos")
	elif st == 8:
		#packStaff
		te = gaussianDistOutlier(12, 3, "pos") if ex == 1 else gaussianDistOutlier(8, 1.8, "pos")
	elif st == 9:
		#shipped
		te = int(randomFloat(60, 75) * 60)
	elif st == 10:
		#shipNotifyDist
		te = gaussianDistOutlier(15, 2, "pos")

	return te

# generate hourly product sales data with daily cycle and gaussian remainder
if __name__ == "__main__":
	op = sys.argv[1]

	if op == "prStat":
		"""
		generate hourly sales stat which is 24 pairs of mean and std deviation
		"""
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
			
	elif op == "prSale":
		"""
		generate hourly sales data with sales statistics mean and std dev already generated		
		"""
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

	elif op == "olPrSale":			
		"""
		insert outliers in sales data for a given outlier rate	and given file with normal sales data
		"""
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
	
	elif op == "scAb":
		"""
		generate abandoned shopping cart data		
		"""
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

	elif op == "olScAb":			
		"""
		insert outliers in abandoned shopping cart data given file with normal shopping cart data
		"""
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
	
	elif op == "updTm":
		"""
		#make timestamp current	
		"""
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
			
	if op == "prStatWkly":
		"""
		generate daily sales stat for each product which is 7 pairs of mean and std deviation
		"""
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

	elif op == "invThOrdQuan":
		"""
		min inventory treshold and re order quantity
		"""
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

	
	elif op == "prSaleInvDaily":
		"""
		generate daily sales and inventory data with sales statistics mean and std dev already generated		
		"""
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
					
	elif op == "OlPrSaleInvDaily":
		"""
		
		"""
		dataFile  = sys.argv[2]
		statFile = sys.argv[3]
		invFile = sys.argv[4]
		olPercent = int(sys.argv[5])
		
	elif op == "ordProcessRecs":
		"""
		states 1 : logged 2: ordered 3: fraud checked 4: manually approved 5:  rejected
		6: order confirmation sent 7: order confirmation from warehouse 8:picked 9: packed
		10: shipped 11: shipment notification sent
		"""
		numOrd = int(sys.argv[2])
		
		visitIntvDist = NormalSampler(1, .2)
		ordPlaceDist = NormalSampler(8, 2)
		frCheckDist = NormalSampler(1, .3)
		manApprovDist = dict()
		manApprovDist[1] = NormalSampler(60, 12)
		manApprovDist[2] = NormalSampler(30, 8)
		confSentDist = NormalSampler(.5, .1)
		warehouseConfirmDist = NormalSampler(10, 1)
		
		pickDist = dict()
		pickDist[1] = NormalSampler(10, 1)
		pickDist[2] = NormalSampler(18, 1.2)
		pickDist[3] = NormalSampler(15, 1.4)
		pickDist[4] = NormalSampler(21, 1.7)
		pickDist[5] = NormalSampler(26, 2.1)
		
		packDist = dict()
		packDist[1] = NormalSampler(12, 3)
		packDist[2] = NormalSampler(8, 1.8)
		
		
		shipNotifyDist = NormalSampler(15, 2)
		
		numCatDist = DiscreteRejectSampler(1, 5, 1, 50, 100, 90, 70, 40)
		
		manApprovStaff = {"HS" : 1, "FN" : 1, "JL" : 2}
		pickStaff = {"JD" : 1, "NT" : 1, "DY" : 1,"GR" : 1, "SR" : 2, "LE" : 2}
		packStaff = {"YF" : 1, "RY" : 1, "IG" : 1, "KJ" : 2, "FY" : 2}
		
		(cTime, pTime) = pastTime(1, "d")
		tm = pTime
		for i in range(numOrd):
			pid = genID(12)
			tm += tmSampleInSec(visitIntvDist)
			ncat = numCatDist.sample()
			
			#logged in
			stm = tm
			ps = -1
			cs = 1
			te = -1
			stm = tm
			ag = "?P"
			aex = -1
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
			
			#order placed
			ps = 1
			cs = 2
			te = tmSampleInSec(ordPlaceDist)
			stm += te
			ag = "?P"
			aex = -1
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
			
			
			#fraud checked
			ps = 2
			cs = 3
			te = tmSampleInSec(frCheckDist)
			stm += te
			ag = "?S"
			aex = -1
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
 
			#fraud detection
			fraudDetected = isEventSampled(5)
			rejected = False
			pstate = 3
			if fraudDetected:
				#manually approved or rejected
				approved = isEventSampled(60)
				ps = 3
				cs = 4 if approved else 5
				staff = selectRandomFromDict(manApprovStaff)
				ag = staff[0]
				aex = staff[1]
				te = tmSampleInSec(manApprovDist[aex])
				stm += te
				print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
				rejected = not approved
				pstate = cs
			
			if rejected:
				continue
			
			#order conformation sent
			ps = pstate
			cs = 6
			te = tmSampleInSec(confSentDist)
			stm += te
			ag = "?S"
			aex = -1
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
			
			 
			#warehouse conformation
			ps = 6
			cs = 7
			te = tmSampleInSec(warehouseConfirmDist)
			stm += te
			ag = "?P"
			aex = -1
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
			
			#picked
			ps = 7
			cs = 8
			staff = selectRandomFromDict(pickStaff)
			ag = staff[0]
			aex = staff[1]
			te = tmSampleInSec(pickDist[ncat])
			if aex == 2:
				te = int(.9 * te - 180)
			stm += te
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
			
			#packed
			ps = 8
			cs = 9
			staff = selectRandomFromDict(packStaff)
			ag = staff[0]
			aex = staff[1]
			te = tmSampleInSec(packDist[aex])
			stm += te
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
			
			#shipped
			ps = 9
			cs = 10
			te = int(randomFloat(0, 60) * 60)
			stm += te
			ag = "?P"
			aex = -1
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
	
			#ship notify 
			ps = 10
			cs = 11
			te = tmSampleInSec(shipNotifyDist)
			stm += te
			ag = "?P"
			aex = -1
			print("{},{},{},{},{},{},{},{}".format(pid, ncat, stm, ps, cs, te, ag, aex))
		
		
	elif op == "olOrdPr":			
		"""
		insert outliers in in order processing  data
		"""
		fileName = sys.argv[2]
		olRate = int(sys.argv[3])
		maCount = 0
		stateOlDist = DiscreteRejectSampler(1, 10, 1, 10, 2, 0, 0, 0, 10, 40, 60, 50, 30)
		olSt = None
		olfile = open("orprol.txt", "w")
		for rec in fileRecGen(fileName, ","):
			olIns = False
			ps = int(rec[3])
			if ps == 4 or ps == 5:
				if maCount < 2:
					aex = int(rec[7])
					te = outlierTimeElapsed(ps, aex, -1)
					rec[5] = str(te)
					maCount += 1
					olIns = True
			else:
				if olSt is None:
					if isEventSampled(olRate):
						olSt = stateOlDist.sample()
				else:
					if ps == olSt:
						aex = int(rec[7])
						ncat = int(rec[1])
						te = outlierTimeElapsed(ps, aex, ncat)
						rec[5] = str(te)
						olSt = None
						olIns = True
					
			mrec = ",".join(rec)
			print(mrec)
			if olIns:
				olfile.write(mrec + "\n")
				
		olfile.close()
		
	elif op == "filtOrdPrOl":			
		"""
		shows expected outliers
		"""
		outFilePath = sys.argv[2]
		olFilePath = sys.argv[3]
		
		tdataOl = getFileLines(olFilePath)
		fnCount = 0
		fpCount = 0
		tpCount = 0
		for rec in fileRecGen(outFilePath):
			r = rec[:-2]
			matched = list(filter(lambda ro : ro == r, tdataOl))
			if len(matched) == 1:
				print(",".join(rec))
				if rec[-1] == "N":
					fnCount += 1
				else:
					tpCount += 1
			else:
				if rec[-1] == "O":
					fpCount += 1
				
		print("false pos {}  false neg {}".format(fpCount, fnCount))	
		preci = tpCount / (tpCount + fpCount)
		recal = tpCount / (tpCount + fnCount)
		print("precision {:.3f}   recall {:.3f}".format(preci, recal))
				
		
		

		
		
				
				

			
			
			
			