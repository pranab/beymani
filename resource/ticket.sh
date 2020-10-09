#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"loadInp")
	rm $PROJECT_HOME/bin/beymani/input/ticket/$3/*
	cp $2 $PROJECT_HOME/bin/beymani/input/ticket/$3/
	ls -l $PROJECT_HOME/bin/beymani/input/ticket/$3/
;;


"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/ticket/train/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/ticket/stat
	rm -rf ./output/ticket/stat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT ticket.conf
;;

"numMstat")
	echo "running NumericalAttrMedian Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrMedian
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/ticket/train/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/ticket/mstat
	rm -rf ./output/ticket/mstat
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT ticket.conf
	rm ./output/ticket/mstat/_SUCCESS
	ls -l ./output/ticket/mstat
;;

"bkMod")
	echo "backing up model files"
	MED_FILES=$PROJECT_HOME/bin/beymani/output/ticket/mstat/*
	META_DIR=$PROJECT_HOME/bin/beymani/meta/ticket
	META_FILE=$META_DIR/$2
	echo "copying to $META_FILE"
	cp /dev/null $META_FILE
	for f in $MED_FILES
	do
  		echo "Copying file $f ..."
  		cat $f >> $META_FILE
	done
	ls -l $META_FILE
;;

"olPred")
	echo "running StatsBasedOutlierPredictor Spark job"
	CLASS_NAME=org.beymani.spark.dist.StatsBasedOutlierPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/ticket/pred/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/ticket/olp
	rm -rf ./output/ticket/olp
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT ticket.conf
	rm ./output/ticket/olp/_SUCCESS
	ls -l ./output/ticket/olp
	cat ./output/ecom/ticket/part-00000 | grep ,O 
;;
	
*) 
	echo "unknown operation $1"
	;;

esac
