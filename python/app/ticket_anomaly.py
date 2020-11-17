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
import matplotlib.pyplot as plt 
import numpy as np
import sklearn as sk
from sklearn.ensemble import IsolationForest
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
from util import *
from sampler import *

"""
Anomaly detection with isolation forest 
"""
if __name__ == "__main__":
	op = sys.argv[1]
	filePath = sys.argv[2]
	scId = sys.argv[3]
	columns = strToIntArray(sys.argv[4])
	window = 20
	beg = 20
	end = beg + window
	if op == "isfo":
		filt = lambda r : r[0] == scId
		data = np.array(getFileAsFiltFloatMatrix(filePath, filt, columns))
		nsamp = data.shape[0]
		isf = IsolationForest(contamination=0.1)
		ypred = isf.fit_predict(data)
		colors = ["m", "g", "b", "c", "y"]
		for i, c in enumerate(columns):
			dvalues = data[:,i]
			if i == 2:
				dvalues = dvalues / 24
			ci = i % 5
			plt.plot(dvalues[beg:end], colors[ci])
		count = 0
		for i in  range(beg, end, 1):
			if ypred[i] == -1:
				plt.axvline(i - beg, 0, .9, color="r")
				count += 1
		print("num of outlier {}".format(count))
		plt.show()
		
		
