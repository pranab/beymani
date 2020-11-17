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

# Package imports
import os
import sys
import random
import statistics 
import matplotlib.pyplot as plt 
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
from util import *
from sampler import *

"""
data generation for web session
"""
if __name__ == "__main__":
	op = sys.argv[1]
	if op == "gen":
		numSamp = int(sys.argv[2])
		if len(sys.argv) == 4:
			percenNormal  = int(sys.argv[3])
		else:
			percenNormal  = -1
			
		hrOfDay = [NormalSampler(14,3), UniformNumericSampler(0,23)]
		numPage = [NormalSampler(12,2.5), NormalSampler(50,5)]
		pageDurAv = [NormalSampler(60, 15), NormalSampler(1,.1)]
		prRevFrac = [NormalSampler(.5,.1), NormalSampler(.9,.05)]
		shopCart = [BernoulliTrialSampler(.6), BernoulliTrialSampler(.2)]
		checkout = [BernoulliTrialSampler(.4), BernoulliTrialSampler(0)]
		logOut = [BernoulliTrialSampler(.8), BernoulliTrialSampler(.95)]
		
		idLists = [genIdList(100, 12), genIdList(80, 12)]
		
		for _ in range(numSamp):
			if percenNormal > 0:
				if isEventSampled(percenNormal):
					di = 0
				else:
					di = 1
			else:
				di = 0
			uid = selectRandomFromList(idLists[di])
			hd = int(hrOfDay[di].sample())
			nup = int(numPage[di].sample())
			pdu = pageDurAv[di].sample()
			prev = prRevFrac[di].sample()
			sc = toIntFromBoolean(shopCart[di].sample())
			co = toIntFromBoolean(checkout[di].sample())
			if di == 1:
				co = 0
			lo = toIntFromBoolean(logOut[di].sample())
			
			print("{},{},{},{:.3f},{:.3f},{},{},{}".format(uid,hd,nup,pdu,prev,sc,co,lo))
			
