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
import math
from datetime import datetime
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
from util import *
from mlutil import *
from daexp import *
from sampler import *

"""
MAchinary vibration time series with multiple harmonic components and  random noise
Inserts outlier with high frequency components indicating failure 
"""

def sinComponents(params):
	"""
	returns list sine components
	"""
	comps = list()
	for i in range(0, len(params), 2):
		amp = params[i]
		per = params[i + 1]
		phase = randomFloat(0, 2.0 * math.pi)
		co = (amp, per, phase)
		comps.append(co)
	return comps

def addSines(comps, sampTm):
	"""
	adds multiple sine comopnents
	"""
	val = 0
	for c in comps:
		t = 2.0 * math.pi * (sampTm % c[1]) / c[1]
		val += c[0] * math.sin(c[2] + t)
	return val
	
if __name__ == "__main__":
	op = sys.argv[1]
	if op == "gen":
		#generate data
		ids = ["HG56SDFE", "K87JG9F6"]
		comps = dict()
		comps["HG56SDFE"] = sinComponents([52,40,76,20,5,80,7,30])
		comps["K87JG9F6"] = sinComponents([56,42,74,18,6,84,9,28])
		noise= NormalSampler(0,3)
		dur = int(sys.argv[2]) * 1000
		ctime = curTimeMs()
		ptime = ctime - dur
		sintv = 1
		stime = ptime
		while stime < ctime:
			for mid in ids:
				val = addSines(comps[mid], stime) + noise.sample()
				print("{},{},{:.3f}".format(mid, stime, val))				
			stime += sintv
					
	elif op == "plot":
		#plot
		fpath = sys.argv[2]
		mid = sys.argv[3]
		beg = int(sys.argv[4])
		end = int(sys.argv[5])
		filt = lambda r : r[0] == mid
		dvalues = list(map(lambda r : float(r[2]), fileFiltRecGen(fpath, filt)))
		drawLine(dvalues[beg:end])
		
	elif op == "iol":
		#insert outliers
		fpath = sys.argv[2]
		delay = int(sys.argv[3]) * 1000 * 2
		ocomps = sinComponents([36,12,30,8])
		i = 0
		for rec in fileRecGen(fpath, ","):
			mid = rec[0]
			if mid ==  "K87JG9F6" and i > delay:
				val = float(rec[2])
				stime = int(rec[1])
				val += addSines(ocomps, stime)
				rec[2] = "{:.3f}".format(val)
			print(",".join(rec))	
			i += 1	
		
	else:
		exitWithMsg("ivalid command")
			
			
		
