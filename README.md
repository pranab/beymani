## Introduction
Beymani consists of set of Hadoop and Storm based tools for outlier and anamoly 
detection, which can be used for fraud detection, intrusion detection. All the 
implementations will be ported to Spark.

## Philosophy
* Simple to use
* Input output in CSV format
* Metadata defined in simple JSON file
* Extremely configurable with tons of configuration knobs

## Blogs
The following blogs of mine are good source of details of beymani
* http://pkghosh.wordpress.com/2012/01/02/fraudsters-outliers-and-big-data-2/
* http://pkghosh.wordpress.com/2012/02/18/fraudsters-are-not-model-citizens/
* http://pkghosh.wordpress.com/2012/06/18/its-a-lonely-life-for-outliers/
* http://pkghosh.wordpress.com/2012/10/18/relative-density-and-outliers/
* http://pkghosh.wordpress.com/2013/10/21/real-time-fraud-detection-with-sequence-mining/
* pkghosh.wordpress.com/2018/09/18/contextual-outlier-detection-with-statistical-modeling-on-spark/
* https://pkghosh.wordpress.com/2018/10/15/learning-alarm-threshold-from-user-feedback-using-decision-tree-on-spark/

## Algorithms
* Multi variate instance distribution model
* Multi variate sequence or multi gram distribution model
* Average instance Distance
* Relative instance Density
* Markov chain with sequence data
* Instance clustering
* Sequence clustering

## Getting started
Project's resource directory has various tutorial documents for the use cases described in
the blogs.

## Build
For Hadoop 1
* mvn clean install

For Hadoop 2 (non yarn)
* git checkout nuovo
* mvn clean install

For Hadoop 2 (yarn)
* git checkout nuovo
* mvn clean install -P yarn

## Help
Please feel free to email me at pkghosh99@gmail.com

## Contribution
Contributors are welcome. Please email me at pkghosh99@gmail.com



