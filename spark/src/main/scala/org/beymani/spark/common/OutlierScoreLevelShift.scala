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

package org.beymani.spark.common

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.hoidla.window.SizeBoundFloatStatsWindow

/**
 * Outlier detection based on level shift outlier score from any algorithm
 * @author pranab
 */
object OutlierScoreLevelShift extends JobConfiguration  with GeneralUtility  {
  
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "outlierScoreLevelShift"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val keyLen = getMandatoryIntParam(appConfig, "key.length", "missing key length")
	   val longWindowSize = getMandatoryIntParam(appConfig, "window.longSize", "missing long window size")
	   val shortWindowSize = getMandatoryIntParam(appConfig, "window.shortSize", "missing short window size")
	   val minZscore = getMandatoryDoubleParam(appConfig, "zscore.min", "missing min z score")
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig,"save.output", true)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   	   
	   val taggedData = data.map(line => {
		 val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		 val key = Record(items, 0, keyLen)
		 (key, items)
	   }).groupByKey.flatMap(r => {
	     val longWindow = new SizeBoundFloatStatsWindow(longWindowSize)
	     val shortWindow = new SizeBoundFloatStatsWindow(shortWindowSize)
	     val values = r._2.toArray.sortBy(v => {
	       v(seqFieldOrd).toLong
	     })
	     val newTags = values.map(v => {
	       val score = v(v.size - 2).toDouble
	       val tag = v(v.size - 1)
	       longWindow.add(score)
	       shortWindow.add(score)
	       var newTag = ""
	       if (longWindow.isFull()) {
	         val loMean = longWindow.getMean()
	         val loStdDev = longWindow.getStdDev()
	         val shMean = shortWindow.getMean()
	         val levelBasedScore = (shMean - loMean) / loStdDev;
	         newTag = if (levelBasedScore > minZscore) "O" else "N"
	       } else {
	         newTag = tag
	       }
	       val rec = Record(2)
	       rec.add(tag,newTag)
	     })
	     
	     //propagate outlier tag
	     for (i <- longWindowSize to newTags.length -1) {
	       if(newTags(i).getString(1) == "O") {
	         for (j <- i - shortWindowSize + 1 to i - 1) {
	           val tag = if (newTags(j).getString(0) == "I") "I" else "O"
	           val rec = Record(2)
	           rec.add(newTags(j).getString(0), tag)
	           newTags(j)  = rec
	         }
	       }
	     }
	     
	     val recValues = values.map(v => Record(v))
	     newTags.zip(recValues).map(r => {
	       val newTag  = r._1.getString(1)
	       val rec = r._2.getString(0)
	       rec + fieldDelimOut + newTag
	     })
	 })
	   
	 if (debugOn) {
         val records = taggedData.collect
         records.slice(0, 100).foreach(r => println(r))
     }
	   
	 if(saveOutput) {	   
	     taggedData.saveAsTextFile(outputPath) 
	 }	 
	   
   }
}