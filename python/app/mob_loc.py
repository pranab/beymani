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
locationTypes = ["residence", "business", "school", "medicalFacility", "shoppingArea", "entertainmentArea", "largeEvent"]
numActivitiesDistr = NonParamRejectSampler(0, 1, 50, 35, 10, 5)
destinationDistr = CategoricalRejectSampler(("medicalFacility", 10), ("shoppingArea", 80), ("entertainmentArea", 30), ("largeEvent", 10))
tripTypeDistr = CategoricalRejectSampler(("multPurpose", 70), ("singlePurpose", 30))
depTimeDistr = NonParamRejectSampler(10, 1, 50, 40, 30, 5, 5, 20, 30, 40, 50, 60, 50, 20, 10)
depTimeDistr.sampleAsFloat()
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
		self.homeLoc =  None
		self.workLoc = None
		self.schoolLoc = None
		self.movements = list()
		self.inTransit = False
		self.transitInitialized = False
		
	def toStr(self):
		print("phoneNum {}  type {}  home {}  work {}  school {}  inTransit {}". \
		format(self.phoneNum, self.type, self.homeLoc, self.workLoc, self.schoolLoc, self.inTransit))
		
		for mv in self.movements:
			mv.toStr()
		
	def setHomeLoc(self, homeLoc):
		self.homeLoc = homeLoc
		
	def setWorkLoc(self, workLoc):
		self.workLoc = workLoc
		
	def setSchoolLoc(self, schoolLoc):
		self.schoolLoc = schoolLoc
		
	def setCurLoc(self, curLoc, locType, arrivalTime):
		self.curLoc = curLoc
		self.locType = locType
		self.arrivalTime = arrivalTime
		self.inTransit = False
		self.transitInitialized = False
		
	def setNextMovement(self, nextMove):
		self.movements.append(nextMove)
	
	def setTripType(self, tripType):
		self.tripType = tripType
		
	def getLoc(self, curTm, sampIntv, allLocations):
		loc = None
		if not self.inTransit:
			if len(self.movements) > 0:
				nextMove = self.movements[0]
				if (curTm < nextMove.depTime):
					loc = self.curLoc
			else:
				loc = self.curLoc
				
			if loc:
				loc = genLatLong(loc[0], loc[1], loc[2], loc[3]) 
			else:
				self.inTransit = True
			
		if self.inTransit:
			#transit
			nextMove = self.movements[0]
			if not self.transitInitialized:
				curLocLat = 0.5 * (self.curLoc[0] + self.curLoc[2])
				curLocLongg = 0.5 * (self.curLoc[1] + self.curLoc[3])
				nextLoc = nextMove.location
				if nextLoc is None:
					self.toStr()
				nextLocType = nextMove.locType
				nextLoc = allLocations[nextLocType][nextLoc]
				nextLocLat = 0.5 * (nextLoc[0] + nextLoc[2])
				nextLocLongg = 0.5 * (nextLoc[1] + nextLoc[3])
			
				dist = geoDistance(curLocLat, curLocLongg, nextLocLat, nextLocLongg)
				self.travelTm =  dist / nextMove.speed
				self.numSteps = int(self.travelTm / sampIntv)
				if self.numSteps == 0:
					self.numSteps = 1
				self.latStep = (nextLocLat - curLocLat) / self.numSteps
				self.longgStep = (nextLocLongg - curLocLongg) / self.numSteps
				self.curSteps = 0
				self.curTransitLoc = [curLocLat, curLocLongg]
				self.transitInitialized = True
			
			if self.curSteps < self.numSteps:
				#in transit
				self.curTransitLoc = [self.curTransitLoc[0] + self.latStep, self.curTransitLoc[1] + self.longgStep]
				loc = self.curTransitLoc
				self.curSteps += 1
			else:
				#arrived
				nextDepTime = nextMove.depTime + self.travelTm + nextMove.timeSpent
				if len(self.movements) > 1:
					self.movements[1].depTime = nextDepTime
				nextLoc = nextMove.location
				nextLocType = nextMove.locType
				nextLoc = allLocations[nextLocType][nextLoc]
				self.setCurLoc(nextLoc, nextLocType, curTm)
				self.movements.pop(0)
				loc = genLatLong(nextLoc[0], nextLoc[1], nextLoc[2], nextLoc[3]) 
				
		return loc
		
class Movement(object):
	"""
	movement event
	"""
	def __init__(self, location, locType, depTime, speed, timeSpent):
		self.location = location
		self.locType = locType
		self.depTime = depTime
		self.speed = (speed * ftPerMile) / secInHour 
		self.timeSpent = timeSpent
		
	def toStr(self):
		print("location {}  locType {}  depTime {} timeSpent {}".format(self.location, self.locType, self.depTime, self.timeSpent ))
		
def loadConfig(configFile):
	"""
	load config file
	"""
	defValues = {}
	defValues["population.num.hours"] = (15, None)
	defValues["population.sampling.interval"] = (5, None)
	defValues["population.size"] = (10000, None)
	defValues["population.num.family"] = (1000, None)
	defValues["population.family.size.mean"] = (3.0, None)
	defValues["population.family.size.sd"] = (.5, None)
	defValues["population.working.family.percentage"] = (70, None)
	defValues["population.retired.one.person.family.percentage"] = (30, None)
	defValues["region.lat.min"] = (2.000, None)
	defValues["region.lat.max"] = (2.000, None)
	defValues["region.long.min"] = (2.000, None)
	defValues["region.long.max"] = (2.000, None)
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
		rec = (float(rec[0]), float(rec[1]), float(rec[2]), float(rec[3]))
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
		members = list()
		if isEventSampled(workingFamPercentage):		
			famSize = int(famSzDistr.sample())
			famSize = 1 if famSize == 0 else famSize
			
			#at least one working member
			pType = "working"
			phNum = genPhoneNum("408")
			person = Person(phNum, pType)
			person.setWorkLoc(sampleUniform(0, numWorkLoc - 1))
			members.append(person)
			
			for j in range(famSize - 1):
				person = createPerson()
				pType = person.type
				if pType == "working":
					person.setWorkLoc(sampleUniform(0, numWorkLoc - 1))
				elif pType == "student":
					person.setSchoolLoc(sampleUniform(0, numSchoolLoc - 1))
				members.append(person)
		else:
			members.append(createRetPerson())
			if not isEventSampled(retOnePersonFamPercentage):
				members.append(createRetPerson())
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
			resList = allLocations["residence"]
			person.setHomeLoc(resList.index(resLoc))
			
			numActivity = numActivitiesDistr.sample()
			workingOrStudent = person.type == "working" or person.type == "student"
			if workingOrStudent and numActivity == 0:
				numActivity = 1
			tripType = tripTypeDistr.sample()
			person.setTripType(tripType)
			if tripType == "singlePurpose" and numActivity > 1:
				numActivity = 1
			firstTrip = True
			speed = float(transportSpeedDistr.sample())
			loc = None
			for i in range(numActivity):
				mv = None
				if person.type == "working" and firstTrip:
					depTime = initTime + sampleUniform(0, secInHour)
					timeSpent = int(timeSpentDistr["business"].sample() * secInMinute)
					loc = person.workLoc
					mv = Movement(loc, "business", depTime, speed, timeSpent)
				
				if person.type == "student" and firstTrip:
					depTime = initTime + sampleUniform(0, 2 * secInHour)
					timeSpent = int(timeSpentDistr["school"].sample() * secInMinute)
					loc = person.schoolLoc
					mv = Movement(loc, "school", depTime, speed, timeSpent)
				
				if mv is None:
					destination = destinationDistr.sample()
					locList = allLocations[destination]
					loc = sampleUniform(0, len(locList) - 1)
					if firstTrip:
						depTime = initTime + (depTimeDistr.sample() - 8) * secInHour
					else:
						depTime = -1
					timeSpent = int(timeSpentDistr[destination].sample() * secInMinute)
					mv = Movement(loc, destination, depTime, speed, timeSpent)	
					
				person.setNextMovement(mv)
				
				if firstTrip:
					firstTrip = False
					
			#back home
			if (numActivity > 0):
				mv = Movement(person.homeLoc, "residence", -1, speed, -1)	
				person.setNextMovement(mv)
				
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
		
		allLocations = dict()
		allLocations["residence"] = residenceLocList
		allLocations["business"] = workLocList
		allLocations["school"] = schoolLocList
		allLocations["medicalFacility"] = medicalFacilityLocList
		allLocations["shoppingArea"] = shoppingAreaLocList
		allLocations["entertainmentArea"] = entertainmentAreaLocList
		allLocations["largeEvent"] = largeEventAreaLocList
		
		families = createFamilies(config, residenceLocList, len(workLocList), len(schoolLocList), personTypes)
		
		pastTm = hourOfDayAlign(pastTm, 8)
		sampTm = pastTm
		curTm = pastTm + 16 * secInHour
		while sampTm < curTm:
			if sampTm == pastTm:
				initPosition(families, allLocations, pastTm)
			else:
				for (resLoc, members) in families.items():
					for person in members:
						loc = person.getLoc(sampTm, smpIntvSec, allLocations)
						print ("{},{},{:.6f},{:.6f}".format(person.phoneNum,sampTm, loc[0], loc[1]))
			sampTm += smpIntvSec
		
		
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
					
		

	
	
	

	


