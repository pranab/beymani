#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
BEYMANI_JAR_NAME=$PROJECT_HOME/bin/beymani/uber-beymani-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"crInput")
	echo "args: num_of_days time_interval(sec) num_of_servers output_file"
	./cpu_usage.py usage $2 $3 $4 true > $5
	ls -l $5
;;

"crTestInput")
	./cpu_usage.py usage $2 $3 $4 true $5 > $6
	ls -l $6
;;

"insOutliers")
	echo "args: normal_data_file  output_file"
	./cpu_usage.py anomaly $2 > $3
	ls -l $3
;;

"cpModData")
	echo "args: modeling_data_file  "
	rm $PROJECT_HOME/bin/beymani/input/olp/*
	rm $PROJECT_HOME/bin/beymani/nas/olp/*
	cp $2 $PROJECT_HOME/bin/beymani/input/nas/
	cp $2 $PROJECT_HOME/bin/beymani/input/olp/
	ls -l $PROJECT_HOME/bin/beymani/input/nas
	ls -l $PROJECT_HOME/bin/beymani/input/olp
;;

"cpTestData")
	echo "args: test_data_file  "
	rm $PROJECT_HOME/bin/beymani/input/olp/*
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
	ls -l $PROJECT_HOME/bin/beymani/output/nas/
;;

"crStatsFile")
	echo "copying and consolidating stats file"
	rm $PROJECT_HOME/bin/beymani/output/nas/_SUCCESS
	SFILE=$PROJECT_HOME/bin/beymani/other/olp/stats.txt
	cp /dev/null $SFILE
	for f in $PROJECT_HOME/bin/beymani/output/nas/*
	do
		echo "Copying file $f ..."
		cat $f >> $SFILE
	done	
	ls -l $PROJECT_HOME/bin/beymani/other/olp
;;

"olPred")
	echo "running StatsBasedOutlierPredictor Spark job"
	CLASS_NAME=org.beymani.spark.dist.StatsBasedOutlierPredictor
	INPUT=file:///Users/pranab/Projects/bin/beymani/input/olp/*
	OUTPUT=file:///Users/pranab/Projects/bin/beymani/output/olp
	rm -rf ./output/olp
	rm -rf ./other/olp/clean
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $BEYMANI_JAR_NAME  $INPUT $OUTPUT and.conf
	rm ./output/olp/_SUCCESS
	for f in ./output/olp/*
	do
		echo "number of records in $f"
		wc -l $f
	done

	for f in ./output/olp/*
	do
		echo "number of outliers in $f"
		cat $f | grep ,O | wc -l
	done
	
;;

"crCleanFile")
	echo "copying, consolidating and moving clean training data file"
	rm $PROJECT_HOME/bin/beymani/other/olp/clean/_SUCCESS
	CFILE=$PROJECT_HOME/bin/beymani/other/olp/clean/cusage.txt
	cp /dev/null $CFILE
	echo "creating clean file $CFILE"
	for f in $PROJECT_HOME/bin/beymani/other/olp/clean/*
	do
  		echo "Copying file $f ..."
  		cat $f >> $CFILE
	done
	echo "copying clean file to model input directory"
	mv $PROJECT_HOME/bin/beymani/input/nas/cusage.txt $PROJECT_HOME/bin/beymani/other/nas/cusage_1.txt 
	mv $CFILE $PROJECT_HOME/bin/beymani/input/nas/cusage.txt
	echo "backing up current model file"
	mv $PROJECT_HOME/bin/beymani/other/olp/stats.txt $PROJECT_HOME/bin/beymani/other/olp/stats_1.txt
	ls -l $PROJECT_HOME/bin/beymani/input/nas/
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