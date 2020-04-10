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

personTypes = ["working", "homeMaker", "student"]
locationTypes = ["residemce", "business", "school", "medicalFacility", "shoppingArea", "entertainmentArea", "largeEvent"]
numActivitiesDistr = NonParamRejectSampler(0, 1, 50, 35, 10, 5)
destinationDistr = CategoricalRejectSampler(("medicalFacility", 10), ("shoppingArea", 80), ("entertainmentArea", 30), ("largeEvent", 10))
tripTypeDistr = CategoricalRejectSampler(("multPurpose", 70), ("singlePurpose", 30))
depTimeDistr = NonParamRejectSampler(10, 1, 50, 40, 30, 5, 5, 20, 30, 40, 50, 60, 50, 20, 10)
transportSpeedDistr = CategoricalRejectSampler(("15", 20), ("30", 50), ("50", 70))
timeSpentDistr = dict()
timeSpentDistr["business"] = GaussianRejectSampler(540, 10)
timeSpentDistr["school"] = GaussianRejectSampler(360, 50)
timeSpentDistr["medicalFacility"] = GaussianRejectSampler(120, 30)
timeSpentDistr["shoppingArea"] = GaussianRejectSampler(30, 5)
timeSpentDistr["entertainmentArea"] = GaussianRejectSampler(90, 20)
timeSpentDistr["largeEvent"] = GaussianRejectSampler(180, 20)

class Person(object):
	"""
	Person location related 
	"""

	def __init__(self, phoneNum, type):
		self.phoneNum = phoneNum
		self.type = type
		self.movements = list()
		
	def setWorkLoc(self, workLoc):
		self.workLoc = workLoc
		
	def setSchoolLoc(self, schoolLoc):
		self.workLoc = workLoc
		
	def setCurLoc(self, curLoc, locType, arrivalTime):
		self.curLoc = curLoc
		locType.locType = locType
		self.arrivalTime = arrivalTime
		
	def setNextMovement(self, nextMove):
		self.movements.append(nextMove)
	
	def setTripType(self, tripType):
		self.tripType = tripType
		
class Movement(object):
	"""
	movement event
	"""
	def __init__(self, location, locType, depTime, speed, timeSpent):
		self. location = location
		self.locType = locType
		self. depTime = depTime
		self. speed = speed
		this.timeSpent = timeSpent
		
		
def loadConfig(configFile):
	"""
	load config file
	"""
	defValues = {}
	defValues["population.num.hours"] = (15, None)
	defValues["population.sampling.interval"] = (5, None)
	defValues["population.size"] = (10000, None)
	defValues["population.num.family"] = (1000, None)
	defValues["population.family.size.mean"] = (2.5, None)
	defValues["population.family.size.sd"] = (.5, None)
	defValues["population.working.family.percentage"] = (70, None)
	defValues["population.retired.family"] = (70, None)
	defValues["population.retired.one.person.family.percentage"] = (30, None)
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
	defValues["region.num.locations"] = (100, None)
	defValues["region.loc.size"] = (0.0002, None)
	defValues["region.quarantine.loc.file"] = (None, None)
	defValues["region.quarantine.num.violation"] = (5, None)
	defValues["region.residence.list.file"] = (None, None)
	defValues["region.work.list.file"] = (None, None)
	defValues["region.school.list.file"] = (None, None)
	defValues["region.medical.facility.list.file"] = (None, None)
	defValues["region.shopping.area.list.file"] = (None, None)
	defValues["region.entertainment.area.list.file"] = (None, None)
	defValues["region.large.event.area.list.file"] = (None, None)

	config = Configuration(configFile, defValues)
	return config

def genLocations(config, minLat, minLong, maxLat, maxLong):
	"""
	generte locations
	"""
	numLoc = config.getIntConfig("region.num.locations")[0]
	locSize = config.getFloatConfig("region.loc.size")[0]
	for i in range(numLoc):
		loc = genLatLong(minLat, minLong, maxLat, maxLong) 
		sampleFloatFromBase(locSize, 0.3 * locSize)
		latMax = loc[0] + sampleFloatFromBase(locSize, 0.3 * locSize)
		longgMax = loc[1] + sampleFloatFromBase(locSize, 0.3 * locSize)
		print ("{:.6f},{:.6f},{:.6f},{:.6f}".format(loc[0], loc[1], latMax, longgMax))

def loadLocations(filePath):
	"""
	load location list
	"""
	locations = list()
	for rec in fileRecGen(filePath, ","):
		rec = (float(rec[1]), float(rec[2]), float(rec[3]), float(rec[4]))
		locations.append(rec)
	return locations

def createFamilies(config, residenceLocList, numWorkLoc, numSchoolLoc, personTypes):
	"""
	creates families
	"""
	families = dict()
	famSizeMn = config.getFloatConfig("population.family.size.mean")[0]
	famSizeSd = config.getFloatConfig("population.family.size.sd")[0]
	famSzDistr = GaussianRejectSampler(famSizeMn,famSizeSd)
	numFamily = config.getFloatConfig("population.num.family")[0]
	workingFamPercentage = config.getIntConfig("population.working.family.percentage")[0]
	retOnePersonFamPercentage = config.getIntConfig("population.retired.one.person.family.percentage")[0]
	for res in residenceLocList:
		memebers = list()
		if isEventSampled(workingFamPercentage):		
			famSize = int(famSzDistr.sample())
			famSize = 1 if famSize == 0 else famSize
			pType = "working"
			phNum = genPhoneNum("408")
			workLoc = sampleUniform(0, numWorkLoc - 1)
			person = Person(phNum, pType, workLoc)
			memebers.append(person)
			for j in range(famSize):
				person = createPerson()
				if pType == "working":
					person.setWorkLoc(sampleUniform(0, numWorkLoc - 1))
				elif pType == "student":
					schoolLoc = sampleUniform(0, numSchoolLoc - 1)
					person.setSchoolLoc(sampleUniform(0, numSchoolLoc - 1))
				memebers.append(person)
		else:
			memebers.append(createRetPerson())
			if not isEventSampled(retOnePersonFamPercentage):
				memebers.append(createRetPerson())
		families[res] = members
		
	return families
			
def createPerson():
	"""
	create person
	"""
	pType = selectRandomFromList(personTypes)
	phNum = genPhoneNum("408")
	person = Person(phNum, pType)
	return person


def createRetPerson():
	"""
	create retired person
	"""
	pType = "retired"
	phNum = genPhoneNum("408")
	person = Person(phNum, pType)
	return person

def initPosition(families, allLocations, initTime):
	"""
	initializes position of a person and all trips for a day
	"""			
	for (resLoc, members) in families.items():
		for person in members:
			person.setCurLoc(resLoc, "residence", initTime)
			numActivity = numActivitiesDistr.sample()
			workingOrStudent = person.type == "working" or person.type == "student"
			if workingOrStudent and numActivity == 0:
				numActivity = 1
			tripType = tripTypeDistr.sample()
			person.setTripType(tripType)
			if tripType == "singlePurpose" and numActivity > 1:
				numActivity = 1
			firstTrip = true
			speed = float(transportSpeedDistr.sample())
			for i in range(numActivity):
				if person.type == "working":
					depTime = initTime + sampleUniform(0, secPerHour)
					timeSpent = int(timeSpentDistr["business"].sample())
					mv = Movement(person.workLoc, "business", depTime, speed, timeSpent)
				elif person.type == "student":
					depTime = initTime + sampleUniform(0, 2 * secPerHour)
					timeSpent = int(timeSpentDistr["school"].sample())
					mv = Movement(person.schoolLoc, "school", depTime, speed, timeSpent)
				else:
					destination = destinationDistr.sample()
					loc = selectRandomFromList(allLocations[destination])
					if firstTrip:
						depTime = intTime + (depTimeDistr.sample() - 8) * secPerHour
					else:
						depTime = -1
					timeSpent = int(timeSpentDistr[destination].sample())
					mv = Movement(person.loc, destination, depTime, speed, timeSpent)	
				person.setNextMovement(mv)
				
				if firstTrip:
					firstTrip = false
					
		

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
	
	
	allLocations = dict()
	if op == "genMovement":
		residenceLocList = loadLocations(config.getStringConfig("region.residence.list.file")[0])
		workLocList = loadLocations(config.getStringConfig("region.work.list.file")[0])
		schoolLocList = loadLocations(config.getStringConfig("region.school.list.file")[0])
		medicalFacilityLocList = loadLocations(config.getStringConfig("region.medical.facility.list.file")[0])
		shoppingAreaLocList = loadLocations(config.getStringConfig("region.shopping.area.list.file")[0])
		entertainmentAreaLocList = loadLocations(config.getStringConfig("region.entertainment.area.list.file")[0])
		largeEventAreaLocList = loadLocations(config.getStringConfig("region.large.event.area.list.file")[0])
		
		allLocations["residemce"] = residenceLocList
		allLocations["business"] = workLocList
		allLocations["school"] = schoolLocList
		allLocations["medicalFacility"] = medicalFacilityLocList
		allLocations["shoppingArea"] = shoppingAreaLocList
		allLocations["entertainmentArea"] = entertainmentAreaLocList
		allLocations["largeEvent"] = largeEventAreaLocList
		
		families = createFamilies(config, residenceLocList, len(workLocList), len(schoolLocList), personTypes)
		
		pastTm = hourOfDayAlign(pastTm, 8)
		sampTm = pastTm
		while sampTm < curTm:
			if sampTm == pastTm:
				initPosition(families, allLocations, pastTm)
			else:
				pass

		
		
	elif op == "genMovementLockdown":
		pass
	elif op == "genLoc":
		genLocations(config, minLat, minLong, maxLat, maxLong)	
	elif op == "genQuaLoc":
		numQu = config.getIntConfig("region.num.locations")[0]
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
	else:
		print("invalid command")
					
		

	
	
	

	


