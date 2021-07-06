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

package org.beymani.spark.multi

import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import scala.collection.mutable.ArrayBuffer
import org.beymani.spark.common.OutlierUtility
import org.chombo.util.BasicUtils

/**
* Implements Isolation forest 
*/
object IsolationForestModel extends JobConfiguration with GeneralUtility with OutlierUtility  {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "isolationForestModel"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val opMode = getStringParamOrElse(appConfig, "op.mode", "norm");
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val attrOrds = toIntegerArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")	
	   val numTree = getIntParamOrElse(appConfig, "num.tree", 100);
	   val subsampleSize = getIntParamOrElse(appConfig, "subsample.size", 100);
	   val defMaxDepth = Math.log(subsampleSize).toInt
	   val maxDepth = getIntParamOrElse(appConfig, "max.depth", defMaxDepth);
	   val countFilePath = getOptionalStringParam(appConfig, "count.filePath")	
	   val modelFilePath = getOptionalStringParam(appConfig, "mod.filePath")	
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val perKeyRecSampCount = countFilePath match {
	     case Some(filePath) => {
	       //many keys
	       val perKeyRecCount = BasicUtils.getKeyedValues(filePath, keyLen, keyLen).asScala
	       val perKeyRecSampCount = perKeyRecCount.map(r => {
	         val rawCount = r._2.toInt
	         val redSubSampSize = (3 * rawCount) / 4
	         val count = if (rawCount > subsampleSize) subsampleSize else redSubSampSize
	         (r._1, count)
	       })
	       perKeyRecSampCount
	     }
	     
	     case None => {
	       //no key
	       val rCount = getMandatoryIntParam(appConfig, "rec.count", "missing record count")
	       val redSubSampSize = (3 * rCount) / 4
	       val count = if (rCount > subsampleSize) subsampleSize else redSubSampSize
	       val perKeyRecSampCount = Map("all" -> count)
	       perKeyRecSampCount
	     }
	   }
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val trRecs = opMode match {
	     case("norm") => {
	       //normal
	       val keyedData = getKeyedValue(data, fieldDelimIn, keyLen, keyFieldOrdinals)
	   
    	   //tree id keyed records
    	   keyedData.groupByKey.flatMap(v => {
    	     val key = v._1
    	     val keyStr = key.toString()
    	     val recs = v._2.map(r => r.getString(0)).toArray
    	     val size = recs.length
    	     val trRecs = ArrayBuffer[(Record, String)]()
    	     val ssSize = perKeyRecSampCount.get(keyStr).get
    	     for (i <- 1 to numTree) {	       
    	       val sampleRecs = if (size > ssSize) {
    	         val samples = Array.ofDim[String](ssSize)
    	         BasicUtils.selectRandomListWithReplacement(recs, samples)
    	         samples
    	       } else {
    	         recs
    	       }
    	       
    	       sampleRecs.foreach(r => {
    	         val tKey = Record(keyLen+2, key)
    	         tKey.addInt(i)
    	         tKey.addString("")
    	         val pair = (tKey, r)
    	         trRecs += pair
    	       })
    	     }
    	     
    	     trRecs
    	   })
	       
	     }
	     case("incr") => {
	       //incremental, exiting path data merged with new incremental data
	       getMandatoryKeyedValue(data, fieldDelimIn, keyLen+2)
	     }
	   }

	   //grow tree
	   var done = false
	   var trPathRecs = trRecs
	   var trExpIter = 1
	   while (!done) {
	     if (debugOn) {
	       println("trExpIter " + trExpIter)
	     }
	     trPathRecs = trPathRecs.groupByKey.flatMap(v => {
	       val key = v._1
	       val recs = v._2.toArray
	       val size = recs.length
	       
	       //current depth
	       //val nonSplittable = (key.getInt(1) >> 8) == 1
	       //val tPath = key.getString(2)
	       //val depth = if (tPath.isEmpty()) 0 else tPath.split(":").length
	       
	       val trPathRecs = ArrayBuffer[(Record, String)]()
	       if (isTerminal(key, maxDepth, size)) {
	         //can not grow any more
	         recs.foreach(v => {
	           val newKey = Record(key)
	           val newVal = new String(v)
	           val pair = (newKey, newVal)
	           trPathRecs += pair
	         })
	       } else {
	         //non terminal
	         var minVal = Double.MaxValue
	         var maxVal = Double.MinValue
	         var found = false
	         var spltAttr = -1
	         val maxTryCnt = 2 * attrOrds.length
	         var tryCnt = 0
	         while(!found && tryCnt < maxTryCnt) {
  	         spltAttr = BasicUtils.selectRandom(attrOrds)
  	         recs.foreach(line => {
  	           val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
  	           val quant = items(spltAttr).toDouble
  	           if (quant < minVal) {
  	             minVal = quant
  	           } 
  	           if(quant > maxVal){
  	              maxVal = quant 
  	           }
  	         })
  	         found = (maxVal - minVal) > .001
  	         if (debugOn && !found){
  	           //println("not enough range in attribute " + spltAttr)
  	         }
  	         tryCnt += 1
	         }
	         
	         if (!found) {
	           //mark  not as not expandable
	           recs.foreach(v => {
	             val newKey = Record(key)
	             var trId = newKey.getInt(1)
	             trId = trId | (1 << 12)
	             newKey.addInt(1, trId)
	             val newVal = new String(v)
	             val pair = (newKey, newVal)
	             trPathRecs += pair
	             if (debugOn) {
	               println("marking node as not expandable path " + newKey.getString(2))
	             }
	           })
	         }
	         else {
	           //split and expand
  	         val splitVal  = minVal + Math.random() * (maxVal - minVal)
  	         if (debugOn) {
  	           println("spltAttr" + spltAttr + " minVal " + minVal + " maxVal " + maxVal + " splitVal " + splitVal)
  	         }
  	         val spKeyLt = "" + spltAttr + "@" + BasicUtils.formatDouble(splitVal, 6) + "@" + "LT"
  	         val spKeyGe = "" + spltAttr + "@" + BasicUtils.formatDouble(splitVal, 6) + "@" + "GE"
  	         val tPathLt = if (key.getString(2).isEmpty()) spKeyLt else key.getString(2) + ":" + spKeyLt
  	         val tPathGe = if (key.getString(2).isEmpty()) spKeyLt else key.getString(2) + ":" + spKeyGe
  	         
  	         recs.foreach(line => {
  	           val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
  	           val quant = items(spltAttr).toDouble
  	           val tPath = if (quant < splitVal) {
  	             tPathLt
  	           } else {
  	             tPathGe
  	           }
  	           val newKey = Record(key)
  	           newKey.addString(2, tPath)
  	           val newVal = new String(line)
  	           val pair = (newKey, newVal)
  	           trPathRecs += pair
  	         })
	         }
	       }
	       trPathRecs
	     })
	     
	     //check if tree building done
	     trPathRecs.cache()
	     val interNodeCount = trPathRecs.groupByKey.map(v => {
	       val key = v._1
	       val recs = v._2.toArray
	       val size = recs.length
	       
	       //current depth
	       val newKey = Record(key)
	       val isInternal = !isTerminal(key, maxDepth, size)
	       if (debugOn) {
	         val tPath = key.getString(2)
	         val depth = if (tPath.isEmpty()) 0 else tPath.split(":").length
	         println("tPath " + tPath + " size " + size + " depth " + depth)
	       }
	       (newKey, isInternal)
	     }).filter(r => r._2).count()
	     
	     done = interNodeCount == 0
	     if (debugOn) {
	       println("interNodeCount " + interNodeCount)
	     }
	     trExpIter += 1
	   }
	   
	   //save model 
	   modelFilePath match {
	     case Some(filePath) => {
	       trPathRecs.cache
	       val trPathRecsStr = trPathRecs.map(r => {
	         //remove non splittable bit and stringify
	         val key = r._1
	         val trID = key.getInt(keyLen) & (1 << 11)
	         key.addInt(keyLen, trID)
	         val value = r._2
	         key.toString(fieldDelimOut) + fieldDelimOut + value
	       })
	       trPathRecsStr.saveAsTextFile(filePath) 
	     }
	     case None =>
	   }

	   //key by partition ID
	   val keyedRecs = trPathRecs.groupByKey.flatMap(v => {
	     val key = v._1
	     val recs = v._2.toArray
	     recs.map(v => {
	       val newKey = Record(keyLen, key)
	       
	       //move tree ID and path to value
	       val newVal = Record(3)
	       newVal.addInt(key.getInt(keyLen))
	       newVal.addString(key.getString(keyLen + 1))
	       newVal.addString(v)
	       (newKey, newVal)
	     })
	   })
	   
	   
	   //anomaly score
	   val taggedRecs = keyedRecs.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val recCount = perKeyRecSampCount.get(keyStr).get
	     val avTreePathLen = avgPathLength(recCount)
	     val recs = v._2.toArray
	     val taggedRecs = ArrayBuffer[String]()
	     if (debugOn){
	       //println("key " + keyStr + " tree recCount " + recCount + " avTreePathLen " + avTreePathLen + "num recs " + recs.length)
	     }
	     
	     //keyed by tree ID and path
	     var tpaRecs = Map[Record, ArrayBuffer[String]]()
	     recs.foreach(v => {
	       val nKey = Record(2, v)
	       val lines = tpaRecs.getOrElse(nKey, ArrayBuffer[String]())
	       lines += v.getString(2)
	       if (lines.length == 1) {
	         tpaRecs += (nKey -> lines)
	       }
	     })
	     
	     //score for each record  from different trees
	     var scores = Map[String, ArrayBuffer[Double]]()
	     tpaRecs.foreach(r => {
	       val tKey = r._1
	       val tPathLen = tKey.getString(1).split(":").length
	       val lines = tpaRecs.get(tKey).get
	       val size = lines.length
	       val score = (if (size == 1) tPathLen else tPathLen + avgPathLength(size)).toDouble
	       if (debugOn) {
	         //println("tPathLen " + tPathLen + " size " + size  + " score " + BasicUtils.formatDouble(score, precision)) 
	       }
	       
	       //val score = tPathLen
	       lines.foreach(line => {
	         val lineScores = scores.getOrElse(line, ArrayBuffer[Double]())
	         if (lineScores.length == 0) {
	           scores += (line -> lineScores)
	         }
	         lineScores += score
	       })
	       
	     })
	       
	     if (debugOn) {
	       println("map size " + scores.size)
	     }
	       
	     //average path length and anomaly score
	     val avScores = scores.map(r => {
	       val avgPathLen = getDoubleSeqAverage(r._2)
	       if (debugOn) {
	         println("av path length " + BasicUtils.formatDouble(avgPathLen, precision))
	       }
	       val score = Math.pow(2.0, - avgPathLen / avTreePathLen)
	       val tag = if (score > scoreThreshold) "O" else "N"
	       val taggedRec = r._1 + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + tag
	       taggedRecs += taggedRec
	     })
	       
	     taggedRecs
	   })
	   
	   if (debugOn) {
       val records = taggedRecs.collect
       records.slice(0, 50).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     taggedRecs.saveAsTextFile(outputPath) 
	   }	 
	   
   }
   
   def avgPathLength(count : Int) : Double = {
     val dCount = count.toDouble
     2.0 * ((Math.log(dCount - 1) + 0.5772156649) -  (dCount - 1) / dCount)
   }
   
   def isTerminal(key : Record, maxDepth : Int, size : Int) : Boolean = {
	   val nonSplittable = (key.getInt(1) >> 12) == 1
	   val tPath = key.getString(2)
	   val depth = if (tPath.isEmpty()) 0 else tPath.split(":").length
     depth == maxDepth || size == 1 || nonSplittable
   }
  
}