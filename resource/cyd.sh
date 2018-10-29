#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/teg/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/mea
	rm -rf ./output/mea
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cyd.conf
;;

"crStatsFile")
	echo "copying and consolidating stats file"
	cat $PROJECT_HOME/bin/beymani/output/mea/part-00000 > $PROJECT_HOME/bin/beymani/other/auc/stats.txt
	cat $PROJECT_HOME/bin/beymani/output/mea/part-00001 >> $PROJECT_HOME/bin/beymani/other/auc/stats.txt
	ls -l $PROJECT_HOME/bin/beymani/other/auc
;;

"tempAggr")
	echo "running TemporalAggregator Spark job"
	CLASS_NAME=org.chombo.spark.explore.TemporalAggregator
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/teg/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/teg
	rm -rf ./output/teg
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cyd.conf
;;

"autoCor")
	echo "running AutoCorrelation Spark job"
	CLASS_NAME=org.chombo.spark.explore.AutoCorrelation
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/auc/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/auc
	rm -rf ./output/auc
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cyd.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac