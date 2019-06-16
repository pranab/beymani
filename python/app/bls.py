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

if __name__ == "__main__":
	op = sys.argv[1]
	
	#device stats
	if op == "stat":
		numDevs = int(sys.argv[2])
		for i in range(numDevs):
			mean = randomFloat(80, 100)
			sd = randomFloat(1, 5)
			devId = genID(12)
			print "%s,%.3f,%.3f" %(devId, mean, sd)
			
	#generate reading		
	elif op == "gen":
		statFile = sys.argv[2]
		devices = []
		for rec in fileRecGen(statFile, ","):
			ds = (rec[0], float(rec[1]), float(rec[2]))
			devices.append(ds)
			
		numDays = int(sys.argv[3])
		numDevs = len(devices)
		distrs = list(map(lambda d: GaussianRejectSampler(d[1],d[2]), devices))	

		curTime = int(time.time())
		pastTime = curTime - (numDays + 1) * secInDay
		pastTime = (pastTime / secInDay) * secInDay + secInHour * 15
		sampTime = pastTime
		sampIntv = secInDay
		
		anm = dict()
		while(sampTime < curTime):
			for i in range(numDevs):
				d = devices[i]
				did = d[0]
				ts = sampTime + randint(-1000, 1000)
				if isEventSampled(10):
					#anomaly
					if isEventSampled(50):
						reading = randomFloat(120, 140)
						high = True
					else:
						reading = randomFloat(60, 80)
						high = False
					anm[did] =  (reading, high)
					#print "**** anomaly created %s, %d" %(did, reading)
				else:
					sampled = False
					if did in anm:
						# moving toward normal from anomaly
						an = anm[did]
						if isEventSampled(60):
							sampled = True
							if an[1]:
								reading = int(0.9 * an[0])
							else:
								reading = int(1.1 * an[0])
							#print "**** moving back to normal %s, %d" %(did, reading)
						del anm[did]
					if not sampled:
						#normal
						reading = distrs[i].sample()
				print "%s,%d,%d" %(did, ts, int(reading))
			sampTime += sampIntv 
				