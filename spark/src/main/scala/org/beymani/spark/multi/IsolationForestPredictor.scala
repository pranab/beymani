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
import scala.util.control.Breaks._
import org.beymani.spark.common.OutlierUtility
import org.chombo.util.BasicUtils

object IsolationForestPredictor extends JobConfiguration with GeneralUtility with OutlierUtility  {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "isolationForestPredictor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val attrOrds = toIntegerArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")	

	   val numTree = getIntParamOrElse(appConfig, "num.tree", 100);
	   val subsampleSize = getIntParamOrElse(appConfig, "subsample.size", 100);
	   val defMaxDepth = Math.log(subsampleSize).toInt
	   val maxDepth = getIntParamOrElse(appConfig, "max.depth", defMaxDepth);
	   val countFilePath = getMandatoryStringParam(appConfig, "count.filePath", "missing per key record count file")	
	   
	   val keyStr = getMandatoryStringParam(appConfig, "target.key", "missing partition key")
	   val modelFilePath = getMandatoryStringParam(appConfig, "mod.filePath", "missing model file path")	
	   val filtModData = getBooleanParamOrElse(appConfig, "mod.filt", true)
	   val filtData = getBooleanParamOrElse(appConfig, "data.filt", true)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val perKeyRecCount = BasicUtils.getKeyedValues(countFilePath, keyLen, keyLen).asScala
	   val perKeyRecSampCount = perKeyRecCount.map(r => {
	     val rawCount = r._2.toInt
	     val count = if (rawCount > subsampleSize) subsampleSize else rawCount
	     (r._1, count)
	   })
	   
	   //model 
	   var model = sparkCntxt.textFile(modelFilePath)
	   model = filtModData match {
	     case true => {
	       model.filter(r => {
	         keyStr.equals(BasicUtils.splitStringOnDelimPosition(r, fieldDelimIn, keyLen, true))
	       })
	     }
	     
	     case false => model
	   }
	   model.cache
	   
	   //input
	   var data = sparkCntxt.textFile(inputPath)
	   data = filtData match {
	     case true => {
	       model.filter(r => {
	         val items = BasicUtils.getTrimmedFields(r, fieldDelimIn)
	         val thisKeyStr = keyFieldOrdinals match {
	           case Some(fieldOrdinals) => BasicUtils.extractFieldsAsString(items , fieldOrdinals, fieldDelimIn)
	           case None => "all"
	         }
	         keyStr.equals(thisKeyStr)
	       })
	     }
	     
	     case false => data
	   }
	   
	   //average path length
	   val avTreePathLen = avgPathLength(data.count().toInt)

	   //pair record with each path of each tree and find path length for matching path
	   val recsPathLen = data.cartesian(model).map(rp => {
	     val rec =  BasicUtils.getTrimmedFields(rp._1, fieldDelimIn)
	     val tpath = rp._2.split(fieldDelimOut)
	     val predicates = tpath(keyLen + 1).split(":")
	     val pathPopCount = tpath(keyLen + 2).toInt
	     
	     var res = true
	     breakable {
  	     predicates.foreach(pred => {
  	       val parts = pred.split("-")
  	       val attr = parts(0).toInt
  	       val prValue = parts(1).toDouble
  	       val op  = parts(2)
  	       
  	       val value = rec(attr).toDouble
  	       val matched = op match {
  	         case "LT" => value < prValue
  	         case "GE" => value >= prValue
  	       }
  	       res = res && matched
  	       if (!res)
  	         break
  	     })
	     }
	     
	     val pLength = if (res) {
	       predicates.length
	     } else {
	       -1
	     }
	     (rp._1, pLength)
	   }).filter(r => r._2 > 0).map(r => {
	    val cntVal = (1, r._2)
	    (r._1, cntVal)
	   })

	   //average path length
	   val recAvPathLen = recsPathLen.reduceByKey( (v1, v2)  => {
	     val cnt = v1._1 + v1._2
	     val pLen = v2._1 + v2._2
	     (cnt, pLen)
	   }).mapValues(r => {r._2.toDouble / r._1 })

	   //outlier score
	   val taggedRecs = recAvPathLen.map(r => {
	     val pLen = r._2
	     val score = Math.pow(2.0, - pLen / avTreePathLen)
	     val tag = if (score > scoreThreshold) "O" else "N"
	     r._1 + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + tag
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
  
}