## Introduction
Beymani consists of set of Hadoop, Spark and Storm based tools for outlier and anamoly 
detection, which can be used for fraud detection, intrusion detection etc.

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
* https://pkghosh.wordpress.com/2018/09/18/contextual-outlier-detection-with-statistical-modeling-on-spark/
* https://pkghosh.wordpress.com/2018/10/15/learning-alarm-threshold-from-user-feedback-using-decision-tree-on-spark/
* https://pkghosh.wordpress.com/2019/07/25/time-series-sequence-anomaly-detection-with-markov-chain-on-spark/
* https://pkghosh.wordpress.com/2020/09/27/time-series-change-point-detection-with-two-sample-statistic-on-spark-with-application-for-retail-sales-data/
* https://pkghosh.wordpress.com/2020/12/24/concept-drift-detection-techniques-with-python-implementation-for-supervised-machine-learning-models/
* https://pkghosh.wordpress.com/2021/01/20/customer-service-quality-monitoring-with-autoencoder-based-anomalous-case-detection/
* https://pkghosh.wordpress.com/2021/06/28/ecommerce-order-processing-system-monitoring-with-isolation-forest-based-anomaly-detection-on-spark/

## Algorithms
* Univarite  distribution model
* Multi variate sequence or multi gram distribution model
* Average instance Distance
* Relative instance Density
* Markov chain with sequence data
* Spectral residue for sequence data
* Quantized symbol mapping for sequence data
* Local outlier factor for multivariate data
* Instance clustering
* Sequence clustering
* Change point detection
* Isolation Forest for multivariate data
* Auto Encoder for multivariate data

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

For Spark
* mvn clean install
* sbt publishLocal
* in ./spark  sbt clean package

## Help
Please feel free to email me at pkghosh99@gmail.com

## Contribution
Contributors are welcome. Please email me at pkghosh99@gmail.com



