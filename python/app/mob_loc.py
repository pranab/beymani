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
sys.path.append(os.path.abspath("../lib"))
from util import *
from mlutil import *
from sampler import *


def loadConfig(configFile):
	"""
	load config file
	"""
	defValues = {}
	defValues["population.num.hours"] = (15, None)
	defValues["population.sampling.interval"] = (5, None)
	defValues["population.size"] = (10000, None)
	defValues["region.lat.min"] = (2.000, None)
	defValues["region.lat.max"] = (2.000, None)
	defValues["region.long.min"] = (2.000, None)
	defValues["region.long.max"] = (2.000, None)
	defValues["region.num.residence"] = (10000, None)
	defValues["region.num.other.facilities"] = (10, None)
	defValues["population.fam.size.mean"] = (3.0, None)
	defValues["population.fam.size.sd"] = (1.0, None)
	defValues["region.num.business"] = (100, None)
	defValues["region.biz.size.mean"] = (20.0, None)
	defValues["region.biz.size.size.sd"] = (3.0, None)
	defValues["region.num.office"] = (20, None)
	defValues["region.office.size.mean"] = (100.0, None)
	defValues["region.biz.size.size.sd"] = (30.0, None)
	defValues["region.num.schools"] = (8, None)
	defValues["region.num.colleges"] = (2, None)
	defValues["region.quarantine.list.file"] = (None, None)
	defValues["region.num.quarantine"] = (100, None)
	defValues["region.loc.size"] = (0.0002, None)
	defValues["region.quarantine.loc.file"] = (None, None)
	defValues["region.quarantine.num.violation"] = (5, None)

	config = Configuration(configFile, defValues)
	return config

if __name__ == "__main__":
	op = sys.argv[1]
	confFile = sys.argv[2]
	config = loadConfig(confFile)
	delim = ","
	numHours = config.getIntConfig("population.num.hours")[0]
	(curTm, pastTm) = pastTime(numHours, "h")
	smpIntv = config.getIntConfig("population.sampling.interval")[0]
	pastTm = multMinuteAlign(pastTm, smpIntv)
	smpIntvSec = smpIntv * 60
	
	minLong = config.getFloatConfig("region.long.min")[0]
	maxLong = config.getFloatConfig("region.long.max")[0]
	minLat = config.getFloatConfig("region.lat.min")[0]
	maxLat = config.getFloatConfig("region.lat.max")[0]
	#print ("{:.6f},{:.6f}, {:.6f},{:.6f}".format(minLat, minLong, maxLat, maxLong))
	
	if op == "normal":
		pass
	elif op == "lockdown":
		pass
	elif op == "genLoc":
		numQu = config.getIntConfig("region.num.quarantine")[0]
		locSize = config.getFloatConfig("region.loc.size")[0]
		inGroup = False
		grSize = 0
		for i in range(numQu):
			phNum = genPhoneNum("408")
			if not inGroup:
				loc = genLatLong(minLat, minLong, maxLat, maxLong) 
			else:
				grCount += 1
				if grCount == grSize:
					inGroup = False
			latMax = loc[0] + locSize
			longgMax = loc[1] + locSize
			print ("{},{:.6f},{:.6f},{:.6f},{:.6f}".format(phNum,loc[0], loc[1], latMax, longgMax))
			if not inGroup:
				if isEventSampled(60):
					inGroup = True
					grSize = sampleUniform(2, 6)
					grCount = 0
					loc = genLatLong(minLat, minLong, maxLat, maxLong) 
		
	elif op == "quaLoc":
		qaListFile = config.getStringConfig("region.quarantine.list.file")[0]
		quRecs = list()
		for rec in fileRecGen(qaListFile, ","):
			rec = (rec[0], float(rec[1]), float(rec[2]), float(rec[3]), float(rec[4]))
			quRecs.append(rec)
	
		sampTm = pastTm
		while sampTm < curTm:
			for qr in quRecs:
				phNum = qr[0]
				lat = qr[1]
				longg = qr[2]
				latMax = qr[3]
				longgMax = qr[4]
				lat = randomFloat(lat, latMax)
				longg = randomFloat(longg, longgMax)
				print ("{},{},{:.6f},{:.6f}".format(phNum,sampTm,lat,longg))
			
			sampTm += smpIntvSec
		
	elif op == "quaLocOutlier":
		qaLocFile = config.getStringConfig("region.quarantine.loc.file")[0]
		numViolation = config.getIntConfig("region.quarantine.num.violation")[0]
		
		#select violator
		qaListFile = config.getStringConfig("region.quarantine.list.file")[0]
		viPhNums = list()
		for rec in fileRecGen(qaListFile, ","):
			viPhNums.append(rec[0])
		
		violationGap =  int((numHours * 60 * 60) / numViolation)
		#print("gap {}".format(violationGap))
		nextTime = None
		violations = dict()
		for rec in fileRecGen(qaLocFile, ","):
			phNum = rec[0]
			tm = int(rec[1])
			if nextTime is None:
				nextTime = tm + sampleUniform(0, 1000)
				viPhNum = selectRandomFromList(viPhNums)
			if tm > nextTime and phNum == viPhNum:
				#new violation
				duration = sampleUniform(4, 10)
				count = 1
				lat = float(rec[2])
				longg = float(rec[3])
				violations[phNum] = (duration, count, lat, longg)
				#print("***** current violation {}".format(phNum))
					
				#next violation
				nextGap = sampleFromBase(violationGap, int(0.6 * violationGap))
				#nextGap = violationGap + sampleUniform(0, 3000)
				nextTime = tm + nextGap
				viPhNum = selectRandomFromList(viPhNums)
				while viPhNum in violations:
					viPhNum = selectRandomFromList(viPhNums)
				#print("***** next violation {} {} {}".format(viPhNum, nextTime, nextGap))
					
			if phNum in violations:
				(duration, count, lat, longg)= violations[phNum]
				count += 1
				lat += randomFloat(0.0020, 0.0025)
				longg += randomFloat(0.0020, 0.0030)	
				rec[2] = "{:.6f}".format(lat)
				rec[3] = "{:.6f}".format(longg)
				if count < duration:
					violations[phNum] = (duration, count, lat, longg)
				else:
					violations.pop(phNum)
					
			print(",".join(rec))
					
		

	
	
	

	


