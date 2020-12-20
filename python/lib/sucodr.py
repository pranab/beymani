#!/usr/local/bin/python3

# beymani-python: Machine Learning
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

# Package imports
import os
import sys
import math
import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import random
import jprops
import statistics as stat
from matplotlib import pyplot
sys.path.append(os.path.abspath("../lib"))
from util import *
from mlutil import *
from sampler import *
from stats import *

class SupConceptDrift(object):
	"""
	supervised cpncept drift detection
	"""
	def __init__(self, threshold):
		self.threshold = threshold
		self.prMin = None
		self.sdMin = None
		self.count = 0
		self.ecount = 0
		self.sum = 0.0
		self.sumSq = 0.0		
		self.diMeanMax = None
		self.diSdMax = None
		self.maxAccRate = None
		
	def ddm(self, values, warmUp=0):
		"""
		DDM algorithm
		"""
		if warmUp > 0:
			for i in range(warmUp):
				if (values[i] == 1):
					self.ecount += 1
				self.count += 1
			self.prMin = self.ecount / self.count
			self.sdMin = math.sqrt(self.prMin * (1 - self.prMin) / self.count )
			#print("min {:.6f},{:.6f}".format(self.prMin, self.sdMin))
		
		result = list()
		for i in range(warmUp, len(values), 1):
			if (values[i] == 1):
				self.ecount += 1
			self.count += 1
			pr = self.ecount / self.count
			sd = math.sqrt(pr * (1 - pr) / self.count)
			dr = 1 if (pr + sd) > (self.prMin + self.threshold * self.sdMin) else 0
			r = (pr, sd, pr + sd, dr)
			result.append(r)
			
			if (pr + sd) < (self.prMin +  self.sdMin):
				self.prMin = pr
				self.sdMin = sd
				#print("counts {},{}".format(self.count, self.ecount))
				#print("min {:.6f},{:.6f}".format(self.prMin, self.sdMin))
			
		return result
			
	def ddmSave(self, fpath):
		"""
		save DDM algorithm state
		"""
		ws = dict()
		ws["count"] = self.count
		ws["ecount"] = self.ecount
		ws["prMin"] = self.prMin
		ws["sdMin"] = self.sdMin
		ws["threshold"] = self.threshold
		saveObject(ws, fpath)
			
	def ddmRestore(self, fpath):
		"""
		restore DDM algorithm state
		"""
		ws = restoreObject(fpath)
		self.count = ws["count"]
		self.ecount = ws["ecount"]
		self.prMin = ws["prMin"]
		self.sdMin = ws["sdMin"]
		self.threshold = ws["threshold"]
		
		
	def eddm(self, values, warmUp=0):
		"""
		EDDM algorithm
		"""
		rstat = RunningStat.create(self.count, self.sum, self.sumSq)
		lastEr = None
		maxLim = 0.0
		result = list()
		if warmUp > 0:
			for i in range(warmUp):
				if (values[i] == 1):
					if lastEr is not None:
						dist = i - lastEr
						rstat.add(dist)
					lastEr = i
				r = (0.0, 0.0, 0.0, 0)
				result.append(r)
			assertGreater(rstat.getCount(), 10, "not enough samples")
			re = rstat.getStat()
			
			self.diMeanMax = re[0]
			self.diSdMax = re[1]
			maxLim = self.diMeanMax + 2.0 * self.diSdMax	
		
		pdr = 0
		for i in range(warmUp, len(values), 1):
			if (values[i] == 1):
				if lastEr is not  None:
					dist = i - lastEr
					re = rstat.addGetStat(dist)
					cur = re[0] + 2.0 * re[1]
					if cur > maxLim:
						self.diMeanMax = re[0]
						self.diSdMax = re[1]
						maxLim = cur
					dr = 1 if (cur / maxLim < self.threshold)  else 0
					r = (re[0],re[1], cur, dr)
					pdr = dr
				else:
					r = (0.0, 0.0, 0.0, pdr)
				lastEr = i
			else:
				r = (0.0, 0.0, 0.0, pdr)		
			result.append(r)								
		
		(self.count, self.sum, self.sumSq) = rstat.getState()
		return result
		
	def eddmSave(self, fpath):
		"""
		save EDDM algorithm state
		"""
		ws = dict()
		ws["count"] = self.count
		ws["sum"] = self.sum
		ws["sumSq"] = self.sumSq
		ws["diMeanMax"] = self.diMeanMax
		ws["diSdMax"] = self.diSdMax
		saveObject(ws, fpath)

	def eddmRestore(self, fpath):
		"""
		restore DDM algorithm state
		"""
		ws = restoreObject(fpath)
		self.count = ws["count"]
		self.sum = ws["sum"]
		self.sumSq = ws["sumSq"]
		self.diMeanMax = ws["diMeanMax"]
		self.diSdMax = ws["diSdMax"]

	def fhddm(self, values, confLevel, winSize=20):
		"""
		FHDDM algorithm
		"""
		result = list()
		accCount = 0
		threshold = math.sqrt(0.5 * math.log(2 / confLevel) * winSize )
		for i in range(winSize):
			if values[i] == 0:
				accCount += 1
		if self.maxAccRate is None:
			self.maxAccRate = accCount / winSize
		else:
			if accRate > self.maxAccRate:
				self.maxAccRate = accRate
			dr = 1 if (self.maxAccRate - accRate) > threshold else 0
			r = (accRate, dr)
			result.append(r)
			
		
		for i in range(winSize, len(values), 1):
			end = i - winSize
			if values[end] == 0:
				accCount -= 1
			if values[i] == 0:
				accCount += 1
			accRate = accCount / winSize
			if accRate > self.maxAccRate:
				self.maxAccRate = accRate
			dr = 1 if (self.maxAccRate - accRate) > threshold else 0
			r = (accRate, dr)
			result.append(r)
		return result

	def fhddmSave(self, fpath):
		"""
		save EDDM algorithm state
		"""
		ws = dict()
		ws["maxAccRate"] = self.maxAccRate
		saveObject(ws, fpath)
			
	def fhddmRestore(self, fpath):
		"""
		restore DDM algorithm state
		"""
		ws = restoreObject(fpath)
		self.maxAccRate = ws["maxAccRate"]
			
			
			
	