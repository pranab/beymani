JAR_NAME=/home/pranab/Projects/avenir/target/avenir-1.0.jar
CLASS_NAME=org.avenir.markov.MarkovStateTransitionModel

echo "running mr"
IN_PATH=/Users/pranab/mmfr/sequence
OUT_PATH=/Users/pranab/mmfr/model
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir"

hadoop jar $JAR_NAME $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/fraud/mmfr.properties $IN_PATH $OUT_PATH
