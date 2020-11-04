#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
AVENIR_JAR_NAME=$PROJECT_HOME/bin/avenir/uber-avenir-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"transformTrain")
	echo "running DataTransformer"
	CLASS_NAME=org.chombo.spark.etl.DataTransformer
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/bsm/train/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/train/trans
	rm -rf ./output/bsm/train/trans
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT bsm.conf
	rm -rf ./output/bsm/train/trans/_SUCCESS
;;

"stateTrans")
	echo "running MarkovStateTransitionModel"
	CLASS_NAME=org.avenir.spark.sequence.MarkovStateTransitionModel
	INPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/train/trans/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/train/sttr
	rm -rf ./output/bsm/train/sttr
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $AVENIR_JAR_NAME  $INPUT $OUTPUT bsm.conf
	rm -rf ./output/bsm/train/sttr/_SUCCESS
;;

"transformPred")
	echo "running DataTransformer"
	CLASS_NAME=org.chombo.spark.etl.DataTransformer
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/bsm/pred/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/pred/trans
	rm -rf ./output/bsm/trans
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT bsm.conf
	rm -rf ./output/bsm/pred/trans/_SUCCESS
;;

"cpModel")
	echo "copying model files"
	MOD_FILES=$PROJECT_HOME/bin/beymani/output/bsm/train/sttr/*
	META_DIR=$PROJECT_HOME/bin/beymani/meta
	cp /dev/null $META_DIR/bsm_mod.txt
	for f in $MOD_FILES
	do
  		echo "Copying file $f ..."
  		cat $f >> $META_DIR/bsm_mod.txt
	done
;;

"olPredict")
	echo "running MarkovChainPredictor"
	CLASS_NAME=org.beymani.spark.seq.MarkovChainPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/pred/trans/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/bsm/pred/oul
	rm -rf ./output/bsm/pred/oul
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT bsm.conf
	rm -rf ./output/bsm/pred/oul/_SUCCESS
	ls -l ./output/bsm/pred/oul
	for f in ./output/bsm/pred/oul/*
	do
		echo "number of  outliers in $f"
		cat $f | grep ,O | wc -l
	done	

;;

*) 
	echo "unknown operation $1"
	;;

esac