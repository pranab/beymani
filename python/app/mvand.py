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
from pyod.models.auto_encoder import AutoEncoder
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
from util import *
from mlutil import *
from sampler import *

"""
Anomaly detection with isolation forest 
"""
if __name__ == "__main__":
	op = sys.argv[1]
	filePath = sys.argv[2]
	window = 20
	beg = 0
	end = beg + window
	if op == "isfo":
		scId = sys.argv[3]
		colStr = sys.argv[4]
		columns = strToIntArray(colStr)
		filt = lambda r : r[0] == scId
		data = np.array(getFileAsFiltFloatMatrix(filePath, filt, colStr))
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
	elif op == "auen":
		teFilePath = sys.argv[3]
		columns = sys.argv[4]
		auen = AutoEncoder(hidden_neurons =[7,5,3,5,7])	
		trData = np.array(getFileAsFloatMatrix(filePath, columns))
		trNsamp = trData.shape[0]
		teData = np.array(getFileAsFloatMatrix(teFilePath, columns))
		aData = np.vstack((trData, teData))
		aData = scaleData(aData, "zscale")
		print(aData.shape)
		trData = aData[:trNsamp, :]
		teData = aData[trNsamp:, :]
		print(trData.shape)
		print(teData.shape)
		
		auen.fit(trData)
		scores = auen.decision_function(teData)
		
		while True:
			inp = input("begin offset: ")
			beg = int(inp)
			end = beg + window
			if beg >= 0:
				plt.plot(scores[beg:end], color="b")
				count = 0
				for i in  range(beg, end, 1):
					if scores[i] > 17:
						plt.axvline(i - beg, 0, .9, color="r")
						count += 1
				print("num of outlier {}".format(count))
				plt.show()
			else:
				print("quitting")
				break
		
		
