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

package org.beymani.multi

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import scala.collection.mutable.ArrayBuffer
import org.beymani.spark.common.OutlierUtility
import org.chombo.util.BasicUtils

object IsolationForestDetector extends JobConfiguration with GeneralUtility with OutlierUtility  {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "IiolationForestDetector"
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
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   val keyedData = getKeyedValue(data, fieldDelimIn, keyLen, keyFieldOrdinals)
	   
	   //tree id keyed records
	   val trRecs = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val recs = v._2.map(r => r.getString(0)).toArray
	     val size = recs.length
	     val trRecs = ArrayBuffer[(Record, String)]()
	     for (i <- 1 to numTree) {	       
	       val sampleRecs = if (size > subsampleSize) {
	         val samples = Array.ofDim[String](subsampleSize)
	         BasicUtils.selectRandomListWithReplacement(recs,samples)
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

	   //grow tree
	   var done = false
	   while (!done) {
	     val trPathRecs = trRecs.groupByKey.flatMap(v => {
	       val key = v._1
	       val recs = v._2.toArray
	       val size = recs.length
	       
	       //current depth
	       val tPath = key.getString(2)
	       val depth = if (tPath.isEmpty()) 0 else tPath.split(":").length
	       
	       val trPathRecs = ArrayBuffer[(Record, String)]()
	       if (depth == maxDepth || size == 1) {
	         //can not grow any more
	         recs.foreach(v => {
	           val newKey = Record(key)
	           val newVal = new String(v)
	           val pair = (newKey, newVal)
	           trPathRecs += pair
	         })
	       } else {
	         val spltAttr = BasicUtils.selectRandom(attrOrds)
	         var minVal = Double.MaxValue
	         var maxVal = Double.MinValue
	         recs.foreach(line => {
	           val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	           val quant = items(spltAttr).toDouble
	           if (quant < minVal) {
	             minVal = quant
	           } 
	           if(quant > minVal){
	              maxVal = quant 
	           }
	         })
	         val splitVal  = minVal + Math.random() * (maxVal - maxVal)
	         val spKeyLt = "" + spltAttr + "-" + BasicUtils.formatDouble(splitVal, 6) + "-" + "LT"
	         val spKeyGe = "" + spltAttr + "-" + BasicUtils.formatDouble(splitVal, 6) + "-" + "GE"
	         recs.foreach(line => {
	           val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	           val quant = items(spltAttr).toDouble
	           val tPath = if (quant < splitVal) {
	             val tPath = if (key.getString(2).isEmpty()) spKeyLt else key.getString(2) + ":" + spKeyLt
	             tPath
	           } else {
	             val tPath = if (key.getString(2).isEmpty()) spKeyLt else key.getString(2) + ":" + spKeyGe
	             tPath
	           }
	           val newKey = Record(key)
	           newKey.addString(2, tPath)
	           val newVal = new String(line)
	           val pair = (newKey, newVal)
	           trPathRecs += pair
	           newKey.addString(2, tPath)
	         })
	         
	       }
	       trPathRecs
	     })
	   }

   }
  
}