#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"numStat")
	echo "running NumericalAttrStats"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/nas/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/nas
	rm -rf ./output/nas
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT and.conf
;;

"createStatsFile")
	cat $PROJECT_HOME/bin/beymani/output/nas/part-00000 > $PROJECT_HOME/bin/beymani/other/olp/stats.txt
	cat $PROJECT_HOME/bin/beymani/output/nas/part-00001 >> $PROJECT_HOME/bin/beymani/other/olp/stats.txt
;;

"olPred")
	echo "running StatsBasedOutlierPredictor"
	CLASS_NAME=org.beymani.spark.dist.StatsBasedOutlierPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/olp/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/olp
	rm -rf ./output/olp
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT and.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac