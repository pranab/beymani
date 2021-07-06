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

/**
* 
* 
*/
object IsolationForestMerge extends JobConfiguration with GeneralUtility with OutlierUtility {
  
   def main(args: Array[String]) {
	   val appName = "isolationForestMerge"
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
	   //val countFilePath = getOptionalStringParam(appConfig, "count.filePath")	
	   val modelFilePath = getMandatoryStringParam(appConfig, "mod.filePath", "missing model file path")	
	   //val precision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   
	   //incremental input
	   val data = sparkCntxt.textFile(inputPath)
	   val keyedData = getKeyedValue(data, fieldDelimIn, keyLen, keyFieldOrdinals)
	   
	   //tree id keyed records
	   val incrRecs = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString()
	     val recs = v._2.map(r => r.getString(0)).toArray
	     val size = recs.length
	     val trRecs = ArrayBuffer[(Record, Record)]()
	     for (i <- 1 to numTree) {	       
	       val sampleRecs = if (size > subsampleSize) {
	         val samples = Array.ofDim[String](subsampleSize)
	         BasicUtils.selectRandomListWithReplacement(recs, samples)
	         samples
	       } else {
	         recs
	       }
	       
	       //key by base ID and tree ID
	       sampleRecs.foreach(r => {
	         val tKey = Record(keyLen+1, key)
	         tKey.addInt(i)
	         val tValue = Record(2)
	         tValue.addString("")
	         tValue.addString(r)

	         val pair = (tKey, tValue)
	         trRecs += pair
	       })
	     }
	     
	     trRecs
	   }).cache()
	   
	   
	   //model data
	   val model = sparkCntxt.textFile(modelFilePath)

	   val modKeyFieldOrdinals = ArrayBuffer[Int]()
	   for (i <- 0 to keyLen - 1) {
	     modKeyFieldOrdinals += i
	   }
	   modKeyFieldOrdinals += keyLen
	   
	   //key by base ID and tree ID
	   val modelRecs = model.map(line => {
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val kLen = modKeyFieldOrdinals.length
	     val key = Record(kLen)
	     
	     for (kf <- modKeyFieldOrdinals) {
	       key.addString(items(kf))
			 }

	     val value = Record(2)
	     value.addString(items(kLen + 1))
	     value.addString(line)
	     (key, value)
	   })	 

	   
	   //megge rnew records in appropriate branch
	   val tPathRecs = modelRecs.union(incrRecs).groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString(fieldDelimOut)
	     val recs = v._2.toArray
	     
	     //new recs
	     val nRecs = recs.filter(v => v.getString(0).isEmpty())
	     
	     //model recs
	     val mRecs = recs.filter(v => v.getString(0).length() > 0)
	     
	     //new path
	     val nPaths = ArrayBuffer[Record]()
	     
	     //create new paths
	     nRecs.foreach(nv => {
	       //new record
	       val items = BasicUtils.getTrimmedFields(nv.getString(1), fieldDelimIn)
	       
	       breakable {
  	       mRecs.foreach(mv => {
  	         //model record
  	         val predicates = mv.getString(0).split(":")
  	         var matched = true
  	         breakable {
    	         predicates.foreach(p => {
    	           val parts = p.split("@")
    	           val pAttr = parts(0).toInt
    	           val pAttrVal = parts(1).toDouble
    	           val pOper = parts(2)
    	           val attrVal = items(pAttr).toDouble
    	           val pMatched = pOper match {
    	             case "LT" => attrVal < pAttrVal
    	             case "GT" => attrVal >= pAttrVal
    	           }
    	           matched = matched && pMatched
    	           if (!matched) {
    	             //failed predicate match, next model record
    	             break
    	           }
    	         })
  	         }
  	         
  	         if (matched) {
  	           //matched all predicates
  	           val tPath = new String(mv.getString(0))
  	           val line = new String(nv.getString(1))
  	           val nPath = Record(2)
  	           nPath.addString(tPath)
  	           nPath.addString(line)
  	           nPaths += nPath
  	           break
  	         }
  	       })
	       }
	     })
	     
	     //combine existing model and new paths
	     val tpRecs = ArrayBuffer[Record]()
	     tpRecs ++= mRecs
	     tpRecs ++= nPaths
	     tpRecs.map(v => {
	       keyStr + fieldDelimOut + v.toString(fieldDelimOut)
	     })
	   })
	   
	   if (debugOn) {
       val records = tPathRecs.collect
       records.slice(0, 50).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     tPathRecs.saveAsTextFile(outputPath) 
	   }	 
	   
	   
   }
}