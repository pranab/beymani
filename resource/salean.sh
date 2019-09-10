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
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/san/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/san/stat
	rm -rf ./output/stat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT salean.conf
;;

"numMstat")
	echo "running NumericalAttrMedian Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrMedian
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/san/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/san/mstat
	rm -rf ./output/san/mstat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT salean.conf
;;

"cpMed")
	echo "copying median files"
	MED_FILES=$PROJECT_HOME/bin/beymani/output/san/mstat/*
	META_DIR=$PROJECT_HOME/bin/beymani/meta/san
	cp /dev/null $META_DIR/$2
	for f in $MED_FILES
	do
  		echo "Copying file $f ..."
  		cat $f >> $META_DIR/$2
	done
	ls -l $META_DIR
;;

"olPred")
	echo "running StatsBasedOutlierPredictor Spark job"
	CLASS_NAME=org.beymani.spark.dist.StatsBasedOutlierPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/san/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/san/olp
	rm -rf ./output/san/olp
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT salean.conf
	echo "number of outliers"
	wc -l ./output/olp/part-00000
	wc -l ./output/olp/part-00001
;;

esac