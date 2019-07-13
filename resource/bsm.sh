#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
AVENIR_JAR_NAME=$PROJECT_HOME/bin/avenir/uber-avenir-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"transformer")
	echo "running DataTransformer"
	CLASS_NAME=org.chombo.spark.etl.DataTransformer
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/bsm/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/trans
	rm -rf ./output/bsm/trans
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT bsm.conf
	rm -rf ./output/bsm/trans/_SUCCESS
;;

"stateTrans")
	echo "running MarkovStateTransitionModel"
	CLASS_NAME=org.avenir.spark.sequence.MarkovStateTransitionModel
	INPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/trans/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/sttr
	rm -rf ./output/bsm/sttr
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $AVENIR_JAR_NAME  $INPUT $OUTPUT bsm.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac