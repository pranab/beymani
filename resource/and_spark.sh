#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"crInput")
	echo "args: num_of_days time_interval(sec) num_of_servers output_file"
	./cpu_usage.py usage $2 $3 $4 > $5
	ls -l
;;

"insOutliers")
	echo "args: normal_data_file  output_file"
	./cpu_usage.py anomaly $2 > $3
	ls -l
;;

"cpModData")
	echo "args: modeling_data_file  "
	cp $2 $PROJECT_HOME/bin/beymani/input/nas/
	cp $2 $PROJECT_HOME/bin/beymani/input/olp/
	ls -l $PROJECT_HOME/bin/beymani/input/nas
	ls -l $PROJECT_HOME/bin/beymani/input/olp
;;

"cpTestData")
	echo "args: test_data_file  "
	cp $2 $PROJECT_HOME/bin/beymani/input/olp/
	ls -l $PROJECT_HOME/bin/beymani/input/olp
;;

"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/nas/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/nas
	rm -rf ./output/nas
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT and.conf
;;

"crStatsFile")
	echo "copying and consolidating stats file"
	cat $PROJECT_HOME/bin/beymani/output/nas/part-00000 > $PROJECT_HOME/bin/beymani/other/olp/stats.txt
	cat $PROJECT_HOME/bin/beymani/output/nas/part-00001 >> $PROJECT_HOME/bin/beymani/other/olp/stats.txt
	ls -l $PROJECT_HOME/bin/beymani/other/olp
;;

"olPred")
	echo "running StatsBasedOutlierPredictor Spark job"
	CLASS_NAME=org.beymani.spark.dist.StatsBasedOutlierPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/olp/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/olp
	rm -rf ./output/olp
	rm -rf ./other/olp/clean
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT and.conf
	echo "number of outliers"
	wc -l ./output/olp/part-00000
	wc -l ./output/olp/part-00001
;;

"crCleanFile")
	echo "copying, consolidating and moving clean training data file"
	cat $PROJECT_HOME/bin/beymani/other/olp/clean/part-00000 > $PROJECT_HOME/bin/beymani/other/olp/clean/cusage.txt
	cat $PROJECT_HOME/bin/beymani/other/olp/clean/part-00001 >> $PROJECT_HOME/bin/beymani/other/olp/clean/cusage.txt
	mv $PROJECT_HOME/bin/beymani/input/nas/cusage.txt $PROJECT_HOME/bin/beymani/other/nas/cusage_1.txt 
	mv $PROJECT_HOME/bin/beymani/other/olp/clean/cusage.txt $PROJECT_HOME/bin/beymani/input/nas/cusage.txt
	mv $PROJECT_HOME/bin/beymani/other/olp/stats.txt $PROJECT_HOME/bin/beymani/other/olp/stats_1.txt
;;


"mvOutlFile")
	echo "moving outlier output file"
	cat $PROJECT_HOME/bin/beymani/output/olp/part-00000 > $PROJECT_HOME/bin/beymani/other/olp/outl.txt
	cat $PROJECT_HOME/bin/beymani/output/olp/part-00001 >> $PROJECT_HOME/bin/beymani/other/olp/outl.txt
;;

"thLearn")
	echo "running ThresholdLearner Spark job"
	CLASS_NAME=org.beymani.spark.common.ThresholdLearner
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/thl/olf.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/thl
	rm -rf ./output/thl
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT and.conf
;;

"tempAggr")
	echo "running TemporalAggregator Spark job"
	CLASS_NAME=org.chombo.spark.explore.TemporalAggregator
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/teg/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/teg
	rm -rf ./output/teg
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT and.conf
;;


*) 
	echo "unknown operation $1"
	;;

esac