#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
AVENIR_JAR_NAME=$PROJECT_HOME/bin/avenir/uber-avenir-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"olPred")
	echo "running SubSequenceDistanceDetector"
	CLASS_NAME=org.beymani.spark.seq.SubSequenceDistanceDetector
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/vib/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/vib
	rm -rf ./output/vib
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT vib.conf
	rm -rf ./output/vib/_SUCCESS
	ls -l ./output/vib
	for f in ./output/vib/*
	do
		echo "number of  outliers in $f"
		cat $f | grep ,O | wc -l
	done	
;;

*) 
	echo "unknown operation $1"
;;

esac