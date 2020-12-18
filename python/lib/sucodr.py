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
import statistics 
from matplotlib import pyplot
sys.path.append(os.path.abspath("../lib"))
from util import *
from mlutil import *
from sampler import *

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

	def ddm(self, values, warmUp=0):
		"""
		DDM algorithm
		"""
		for i in range(warmUp):
			if (values[i] == 1):
				self.ecount += 1
			self.count += 1
		self.prMin = self.ecount / self.count
		self.sdMin = math.sqrt(self.prMin * (1 - self.prMin) / self.count )
		print("min {:.6f},{:.6f}".format(self.prMin, self.sdMin))
		
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
				print("counts {},{}".format(self.count, self.ecount))
				print("min {:.6f},{:.6f}".format(self.prMin, self.sdMin))
			
		return result
			
			
		
		
	