#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"cpQuaLocData")
	echo "args: data_file  "
	cp $2 $PROJECT_HOME/bin/beymani/other/epid/$3/
	ls -l $PROJECT_HOME/bin/beymani/other/epid/$3/
;;


"cpLocData")
	echo "args: test_data_file  "
	cp $2 $PROJECT_HOME/bin/beymani/input/epid/$3/
	ls -l $PROJECT_HOME/bin/beymani/input/epid/$3/
;;

"olPredOu")
	echo "running OutRangeBasedPredictor Spark job"
	CLASS_NAME=org.beymani.spark.misc.OutRangeBasedPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/outr/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/epid/outr
	rm -rf ./output/epid/outr
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT epid.conf
	echo "number of outliers"
	wc -l ./output/epid/outr/part-00000
	wc -l ./output/epid/outr/part-00001
;;

"olPredIn")
	echo "running InRangeBasedPredictor Spark job"
	CLASS_NAME=org.beymani.spark.misc.InRangeBasedPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/epid/inr/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/epid/inr
	rm -rf ./output/epid/inr
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT epid.conf
	echo "number of outliers"
	wc -l ./output/epid/inr/part-00000
	wc -l ./output/epid/inr/part-00001
;;

*) 
	echo "unknown operation $1"
	;;

esac