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
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/ecom/training/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/ecom/stat
	rm -rf ./output/ecom/stat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT ecSale.conf
;;

"numMstat")
	echo "running NumericalAttrMedian Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrMedian
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/ecom/training/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/ecom/mstat
	rm -rf ./output/ecom/mstat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT ecSale.conf
	ls -l ./output/ecom/mstat
;;

"bkMod")
	echo "backing up model files"
	MED_FILES=$PROJECT_HOME/bin/beymani/output/ecom/mstat/*
	META_DIR=$PROJECT_HOME/bin/beymani/meta/ecom
	META_FILE=$META_DIR/$2
	cp /dev/null $META_FILE
	for f in $MED_FILES
	do
  		echo "Copying file $f ..."
  		cat $f >> $META_FILE
	done
	ls -l $META_DIR
;;

"cpMod")
	echo "copying model files files from backup"
	META_DIR=$PROJECT_HOME/bin/beymani/meta/ecom
	cp $META_DIR/$2/*  $META_DIR/
	ls -l $META_DIR
;;

"olPred")
	echo "running StatsBasedOutlierPredictor Spark job"
	CLASS_NAME=org.beymani.spark.dist.StatsBasedOutlierPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/ecom/pred/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/ecom/olp
	rm -rf ./output/ecom/olp
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT ecSale.conf
	ls -l ./output/ecom/olp
;;

"chkOl")
	echo "number of outliers"
	OUT_FILES=$PROJECT_HOME/bin/beymani/output/ecom/olp/*
	for f in $OUT_FILES
	do
  		echo "checking file $f ..."
  		wc -l $f
	done
;;

"bkOut")
	echo "backing up output files"
	OUT_FILES=$PROJECT_HOME/bin/beymani/output/ecom/olp/*
	BK_DIR=$PROJECT_HOME/bin/beymani/output/ecom
	BK_FILE=$BK_DIR/$2
	cp /dev/null $BK_FILE
	for f in $OUT_FILES
	do
  		echo "Copying file $f ..."
  		cat $f >> $BK_FILE
	done
	ls -l $BK_DIR
;;



esac