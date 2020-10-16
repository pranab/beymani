#!/usr/local/bin/python3

import os
import sys
from random import randint
import time
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

op = sys.argv[1]
secInHour = 60 * 60
secInDay = 24 * secInHour
secInWeek = 7 * secInDay
secInYear = 365 * secInDay

wkDayMean = 60
wkDayStdDev = 12
wkEndMean = 30
wkEndStdDev = 8

if op == "usage":
	numDays = int(sys.argv[2])
	sampIntv = int(sys.argv[3])
	numServers = int(sys.argv[4])
	
	outDayInWeek = True
	s = 5
	if len(sys.argv) > 5:
		#print(sys.argv[5])
		if sys.argv[5] == "false" or sys.argv[5] == "f":
			outDayInWeek = False
		s = 6		

	serverList = None
	if len(sys.argv) > s:
		#server ID from stats file
		sfile = sys.argv[s]
		#print(sfile)
		servers = set()
		for rec in fileRecGen(sfile, ","):
			#print(rec[0])
			servers.add(rec[0])
		serverList = list(servers)
	else:
		#generate server ID
		serverList = list()
		for i in range(numServers):
			serverList.append(genID(10))
	
	curTime = int(time.time())
	pastTime = curTime - (numDays + 1) * secInDay
	sampTime = pastTime
	usageDistr = [GaussianRejectSampler(wkDayMean,wkDayStdDev), GaussianRejectSampler(wkEndMean,wkEndStdDev)]

	while(sampTime < curTime):
		secIntoDay = sampTime % secInDay
		#hourIntoDay = secIntoDay / secInHour
	
		secIntoWeek = sampTime % secInWeek
		daysIntoWeek = int(secIntoWeek / secInDay)
	
		if daysIntoWeek >= 0 and daysIntoWeek <= 4:
			distr = usageDistr[0]
		else:
			distr = usageDistr[1]
		
		for server in serverList:
			usage = distr.sample()
			if (usage < 0):
				usage = 5
			elif usage > 100:
				usage = 100
			usage = int(usage)			
			st = sampTime + randint(-2,2)
			if outDayInWeek:
				#print "%s,%d,%d,%d" %(server, st, daysIntoWeek, usage)
				print("{},{},{},{}".format(server, st, daysIntoWeek, usage))
			else:
				#print "%s,%d,%d" %(server, st, usage)
				print("{},{},{}".format(server, st, usage))
				
		sampTime = sampTime + sampIntv
	
elif op == "anomaly":
	fileName = sys.argv[2]
	count = 0
	for rec in fileRecGen(fileName, ","):
		if isEventSampled(8):
			dow = int(rec[2])
			if dow < 5:
				rec[3] = str(randint(94, 100))
			else:
				rec[3] = str(randint(54, 100))
			count += 1
		mrec = ",".join(rec)
		print(mrec)
	#print "num of anomalous records " + str(count)

elif op == "feedback":
	fileName = sys.argv[2]
	curThreshold = float(sys.argv[3])
	newThreshold = float(sys.argv[4])
	margin = curThreshold + 0.6 * (newThreshold - curThreshold)
	count = 0
	for rec in fileRecGen(fileName, ","):
		score = float(rec[4])	
		label = rec[5]
		if newThreshold > curThreshold:
			#false positive
			if label == "O":
				if score > newThreshold:
					flabel = "O"
					cl = "T"
				else:
					if score < margin or isEventSampled(90):
						flabel = "N"
						cl = "F"
						count += 1
					else:
						flabel = "O"
						cl = "T"
			else:
				flabel = "N"
				cl = "F"
		else:
			#false negative
			if label == "O":
				flabel = "O"
				cl = "T"
			else:
				if score > newThreshold:
					if score > margin or isEventSampled(90):
						flabel = "O"
						cl = "T"
						count += 1
					else:
						flabel = "N"
						cl = "F"			
				else:
					flabel = "N"
					cl = "F"
		rec.append(flabel)
		rec.append(cl)
		mrec = ",".join(rec)
		print(mrec)
	#print count	
	
elif op == "addTrend":
	fileName = sys.argv[2]
	trendYearlyPercentRate = float(sys.argv[3])
	trenPerSec = trendYearlyPercentRate / secInYear
	start = None
	for rec in fileRecGen(fileName, ","):
		ts = int(rec[1])
		usage = float(rec[3])
		if start is None:
			start = ts
		else:
			usage = usage + (ts - start) * trenPerSec
	usageStr = ".3f" %(usage)
	rec[3] = usageStr
	mrec = ",".join(rec)
	print(mrec)
			
			

			