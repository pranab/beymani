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
import org.apache.spark.rdd.RDD
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import scala.collection.mutable.ArrayBuffer
import org.beymani.spark.common.OutlierUtility
import org.chombo.util.BasicUtils

/**
* local neighborhood density based outlier predictor aka Local Outlier Factor
* @param args
* @return
*/
object LocalDensityBasedPredictor extends JobConfiguration with GeneralUtility with OutlierUtility  {
  
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
	   val fullFilePath = getOptionalStringParam(appConfig, "full.filePath")
	   val keyLen = getMandatoryIntParam(appConfig, "key.field.length","missing key field length") 
	   val neighborCount = getIntParamOrElse(appConfig, "nearest.neighbor.count", 3)
	   val glScoreThreshold = getOptionalDoubleParam(appConfig, "score.threshold")
	   val keyBasedScoreThreshold = glScoreThreshold match {
	     case Some(th) => None
	     case None => {
	       val statsPath = getMandatoryStringParam(appConfig, "stats.file.path", "missing stat file path")
	       val statFldOrd = getMandatoryIntParam(appConfig, "stats.field.ordinal","missing stats field ordinal")
	       val scoreThresholdMap = BasicUtils.getKeyedValues(statsPath, keyLen, statFldOrd)
	       Some(scoreThresholdMap)
	     }
	   }
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	   //distance input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //nearest k neighbors
	   val pairDistance = data.flatMap(line => {
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val recs = Array.ofDim[(Record, Record)](2)
	     
	     //seq ID added to key
	     var key = Record(keyLen + 1)
	     populateFields(items, keyLen, key)
	     key.addLong(items(keyLen).toLong)	     
	     var value = Record(2)
	     value.addLong(items(keyLen+1).toLong)
	     value.addDouble(items(keyLen+2).toDouble)
	     recs(0) = (key, value)
	     
	     //seq ID added to key
	     key = Record(keyLen + 1)
	     populateFields(items, keyLen, key)
	     key.addLong(items(keyLen + 1).toLong)	     
	     value = Record(2)
	     value.addLong(items(keyLen).toLong)
	     value.addDouble(items(keyLen+2).toDouble)
	     recs(1) = (key, value)
	     recs
	   }).cache
	   
	   //nearest k neighbors
	   val kNeighbors = pairDistance.groupByKey.map(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val values = v._2.toList.sortBy(v => v.getDouble(1)).take(neighborCount)
	     (key, values)
	   }).cache
	   
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
	   val lrd = rDist.map(r => {
	     val key = Record(keyLen + 1, r._1, 0, keyLen+1)
	     val value = Record(2)
	     value.addInt(1)
	     value.addDouble(r._2.getDouble(0))
	     (key, value)
	   }).reduceByKey((v1, v2)  => {
	     val v = Record(2)
	     v.addInt(v1.getInt(0) + v2.getInt(0))
	     v.addDouble(v1.getDouble(1) + v2.getDouble(1))
	     v
	   }).mapValues(r => {
	     val den = 1.0 / (r.getDouble(1) / r.getInt(0))
	     createDoubleFieldRec(den)
	   })
	   
	   //flatten source and k neighbors
	   def flatNeighbors(ne : RDD[(Record, List[Record])]) :  RDD[(Record, Record)] = {
	     ne.flatMap(r => {
	       val flNeighbors = ArrayBuffer[(Record, Record)]()
	       val k = Record(r._1)
	       val v = Record(2)
	       v.addLong(r._1.getLong(keyLen))
	       v.addDouble(0)
	       val nr = (k,v)
	       flNeighbors += nr
	       r._2.foreach(n => {
	         val k = Record(keyLen + 1, r._1, 0, keyLen)
	         k.add(n.getLong(0))
	         val v = Record(2)
	         v.addLong(r._1.getLong(keyLen))
	         v.addDouble(0)
	         val nr = (k,v)
	         flNeighbors += nr
	       })
	       flNeighbors
	     })
	   }
	   
	   //find lof
	   val taggedRecs = lrd.union(flatNeighbors(kNeighbors)).groupByKey.flatMap(r => {
	     //populate soure and each neighbor with lrd
	     val values = r._2.toArray
	     val lrdArr = values.filter(v => v.size == 1)
	     BasicUtils.assertCondition(lrdArr.length == 1, "record containing lrd should have size 1")
	     val lrd = lrdArr(0).getDouble(0)
	     val nValues = values.filter(v => v.size == 2).map(v => {
	       //put seq of neighborhood center in key
	       v.addDouble(1, lrd)
	       val k = Record(keyLen + 1, r._1, 0, keyLen)
	       k.addLong(v.getLong(0))
	       v.addLong(0, k.getLong(keyLen))
	       (k, v)
	     })
	     nValues
	   }).groupByKey.map(r => {
	     //calculate olf
	     val ncSeq = r._1.getLong(keyLen)
	     val values = r._2.toArray
	     val nLrds = values.filter(r => r.getLong(0) != ncSeq).map(r => r.getDouble(1))
	     val cLrdRec = values.find(r => r.getLong(0) == ncSeq)
	     val cLrd = cLrdRec match {
	       case Some(c) => c.getDouble(1)
	       case None => throw new IllegalStateException("")
	     }
	     val olf = (nLrds.sum / nLrds.length) / cLrd 
	     val keyStr = r._1.toString(0, keyLen, fieldDelimOut)
	     val label = getOutlierLabel(keyStr, olf, "high", glScoreThreshold, keyBasedScoreThreshold, false)
	     r._1.toString(fieldDelimOut) + fieldDelimOut + BasicUtils.formatDouble(olf, precision) + 
	       fieldDelimOut + label
	       
	     val k = Record(r._1)
	     val v = Record(2)
	     v.addDouble(olf)
	     v.addString(label)
	     (k, v)
	   }).cache
	   
	   //use full records
	   val fullRecs = fullFilePath match {
	     case Some(path) => {
	       val data = sparkCntxt.textFile(path)
	       val keyedRecs =  getKeyedValue(data, fieldDelimIn, keyLen, keyFieldOrdinals)
	       keyedRecs.union(taggedRecs).reduceByKey((r1, r2) => {
	         if (r1.size == 2) {
	           val v = Record(3, r1, 1)
	           v.addString(0, r2.getString(0))
	         } else {
	           val v = Record(3, r2, 1)
	           v.addString(0, r1.getString(0))
	         }
	       })
	     } 
	     case None => {
	       taggedRecs
	     }
	   }
	   val taggedFullRecs = fullRecs.map(r => r._1.toString(fieldDelimOut) + fieldDelimOut + r._2.toString(fieldDelimOut))
	   if (debugOn) {
         val records = taggedFullRecs.collect
         records.slice(0, 20).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     taggedFullRecs.saveAsTextFile(outputPath) 
	   }	 
	   
   } 
}