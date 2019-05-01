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

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import org.chombo.util.BaseAttribute
import com.typesafe.config.Config

/**
 * Finds threshold based pseudo relevance e.g. top n or top n percentage
 * @author pranab
 *
 */
object PseudoRelevanceThresholdFinder extends JobConfiguration {
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "outlierCounter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyLen = getMandatoryIntParam(appConfig, "data.keyLen", "missing key length")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val relevanceThreshold = getMandatoryDoubleParam(appConfig, "relevance.threshold", "missing relevance threshold")
	   val relevanceAsPercentage = getBooleanParamOrElse(appConfig, "relevance.asPercentage", true)
	   val minSampleCount = getMandatoryIntParam(appConfig, "sample.minCount", "missing min sample count")
	   val thresholdPath = getMandatoryStringParam(appConfig, "threshold.filePath", "missing stat file path")
	   val thresholdMap = BasicUtils.getKeyedValues(thresholdPath, keyLen, keyLen)
	   val defaultThreshold = getMandatoryDoubleParam(appConfig, "threshold.default", "missing default threshold")
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)

	   val keyedThresholds = data.map(line => {
   		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
   		   val keyRec = Record(items, 0, keyLen)
   		   val last = items.length - 1
   		   val score = items(last -1).toDouble
   		   (keyRec, score)
	   }).groupByKey.map(r => {
	     val key = r._1
	     val scores = r._2.toList
	     val sortedScores = scores.sortWith((v1,v2) => v1 > v2)
	     val size = sortedScores.length
	     val threshold = 
	     if (size > minSampleCount) {
	         //find threshold
		     val thresholdIndex = 
		     if (relevanceAsPercentage) {
		       ((size * relevanceThreshold) / 100).toInt - 1
		     } else {
		       val indx = relevanceThreshold.toInt - 1
		       if (indx > size-2) {
		         throw new IllegalStateException("absolute threshold value too big")
		       }
		       indx
		     }
		     sortedScores.slice(thresholdIndex - 1, 3).sum / 3
	     } else {
	       //use existing threshold or default
	       val keyStr = key.toString(fieldDelimOut)
	       if (thresholdMap.containsKey(keyStr)) thresholdMap.get(keyStr).toDouble
	       else defaultThreshold
	     }
	     key.toString(fieldDelimOut) + fieldDelimOut + BasicUtils.formatDouble(threshold, precision)
	   })
	   
       if (debugOn) {
         val records = keyedThresholds.collect.slice(0, 20)
         records.foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     keyedThresholds.saveAsTextFile(outputPath) 
	   }
	   
   }
}