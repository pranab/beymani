JAR_NAME=/home/pranab/Projects/beymani/target/beymani-1.0.jar
CLASS_NAME=org.beymani.proximity.AverageDistance

echo "running mr"
IN_PATH=/user/pranab/cct/simi
OUT_PATH=/user/pranab/cct/negr
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir"

hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/fraud/cct.properties  $IN_PATH  $OUT_PATH
