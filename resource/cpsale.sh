#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash.local:7077

case "$1" in

"cpInp")
	echo "args: data_file  "
	cp $2 $PROJECT_HOME/bin/beymani/input/cpsale/
	ls -l $PROJECT_HOME/bin/beymani/input/cpsale/
;;

"olPredOu")
	echo "running ChangePointDetector Spark job"
	CLASS_NAME=org.beymani.spark.misc.ChangePointDetector
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/cpsale/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/cpsale
	rm -rf ./output/cpsale
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT epid.conf
	wc -l ./output/epid/cpsale/part-00000
	wc -l ./output/epid/cpsale/part-00001
;;


*) 
	echo "unknown operation $1"
	;;

esac