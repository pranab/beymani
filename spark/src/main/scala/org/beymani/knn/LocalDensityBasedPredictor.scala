/*
 * beymani-spark: Outlier and anamoly detection 
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.beymani.knn

import scala.math.max
import org.apache.spark.SparkContext
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils

/**
* local neighborhood density based outlier predictor aks Local Outlier Factor
* @param args
* @return
*/
object LocalDensityBasedPredictor extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "localDensityBasedPredictor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val seqFieldOrdinal = getMandatoryIntParam(appConfig, "seq.field.ordinal","missing sequence field ordinal") 
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val distFilePath = getMandatoryStringParam(appConfig, "dist.file.path","missing distance file path")
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val neighborCount = getIntParamOrElse(appConfig, "nearest.neighbor.count", 3)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	   //distance input
	   val data = sparkCntxt.textFile(distFilePath)
	   
	   //nearest k neighbors
	   val pairDistance = data.flatMap(line => {
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val recs = Array.ofDim[(Record, Record)](2)
	     
	     var key = Record(keyLen + 1)
	     populateFields(items, keyLen, key)
	     key.addLong(items(keyLen).toLong)	     
	     var value = Record(2)
	     value.addLong(items(keyLen+1).toLong)
	     value.addDouble(items(keyLen+2).toDouble)
	     recs(0) = (key, value)
	     
	     key = Record(keyLen + 1)
	     populateFields(items, keyLen, key)
	     key.addLong(items(keyLen + 1).toLong)	     
	     value = Record(2)
	     value.addLong(items(keyLen).toLong)
	     value.addDouble(items(keyLen+2).toDouble)
	     recs(1) = (key, value)
	     recs
	   }).cache
	   
	   val kNeighbors = pairDistance.groupByKey.map(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val values = v._2.toList.sortBy(v => v.getDouble(1)).take(neighborCount)
	     (key, values)
	   })
	   
	   //distance to kth nearest neighbor
	   val knDist = kNeighbors.map(r => {
	     val dist = r._2(neighborCount - 1).getDouble(1)
	     val value = createDoubleFieldRec(dist)
	     (r._1, value)
	   }).cache
	   
	   //reachability distance
	   val rDist = pairDistance.union(knDist).reduceByKey((v1, v2) => {
	     //max of k distance and distance
	     if (v1.size == 2) {
	       //d1:dist  d2:kdist
	       val d1 = v1.getDouble(1)
	       val d2 = v2.getDouble(0)
	       val v = Record(2)
	       v.addLong(v1.getLong(0))
	       v.addDouble(max(d1,d2))
	       v
	     } else {
	       //d1:dist  d2:kdist
	       val d1 = v2.getDouble(1)
	       val d2 = v1.getDouble(0)
	       val v = Record(2)
	       v.addLong(v2.getLong(0))
	       v.addDouble(max(d1,d2))
	       v
	     }
	   }).map(r => {
	     val key = Record(keyLen + 2, r._1, 0, keyLen)
	     key.addLong(r._2.getLong(0))
	     key.addLong(r._1.getLong(keyLen))
	     val value = createDoubleFieldRec(r._2.getDouble(1))
	     (key, value)
	   })
	   
	   //local reachability density
	   
   } 
}