JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
CLASS_NAME=org.chombo.mr.Projection

echo "running mr"
IN_PATH=/Users/pranab/mmfr/input
OUT_PATH=/Users/pranab/mmfr/sequence
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir"

hadoop jar $JAR_NAME $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/fraud/mmfr.properties $IN_PATH $OUT_PATH
