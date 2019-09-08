#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

*) 
	echo "unknown operation $1"
	;;


"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/saen/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/saen/stat
	rm -rf ./output/stat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT salean.conf
;;

"numMstat")
	echo "running NumericalAttrMedian Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrMedian
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/saen/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/saen/mstat
	rm -rf ./output/mstat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT salean.conf
;;

"olPred")
	echo "running StatsBasedOutlierPredictor Spark job"
	CLASS_NAME=org.beymani.spark.dist.StatsBasedOutlierPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/saen/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/saen/olp
	rm -rf ./output/saen/olp
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT and.conf
	echo "number of outliers"
	wc -l ./output/olp/part-00000
	wc -l ./output/olp/part-00001
;;

esac