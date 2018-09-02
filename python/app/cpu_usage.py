#!/usr/bin/python

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


if op == "usage":
	numDays = int(sys.argv[2])
	sampIntv = int(sys.argv[3])
	numServers = int(sys.argv[4])

	serverList = []
	for i in range(numServers):
		serverList.append(genID(10))
	
	curTime = int(time.time())
	pastTime = curTime - (numDays + 1) * secInDay
	sampTime = pastTime
	usageDistr = [GaussianRejectSampler(60,12), GaussianRejectSampler(30,8)]

	while(sampTime < curTime):
		secIntoDay = sampTime % secInDay
		#hourIntoDay = secIntoDay / secInHour
	
		secIntoWeek = sampTime % secInWeek
		daysIntoWeek = secIntoWeek / secInDay
	
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
						
			st = sampTime + randint(-2,2)
			print "%s,%d,%d,%d" %(server, st, daysIntoWeek, usage)
		
		sampTime = sampTime + sampIntv
	
elif op == "anomaly":
	fileName = sys.argv[2]
	count = 0
	for rec in fileRecGen(fileName, ","):
		if isEventSampled(10):
			rec[3] = str(randint(98, 100))
			count += 1
		mrec = ",".join(rec)
		print mrec
	#print "num of anomalous records " + str(count)
	
	 	