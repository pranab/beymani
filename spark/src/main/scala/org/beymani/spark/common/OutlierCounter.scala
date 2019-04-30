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
* Outlier count statistics
* @author pranab
*
*/
object OutlierCounter extends JobConfiguration {
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
	   val insertTimeStamp = getBooleanParamOrElse(appConfig, "output.insertTmStmp", false)
	   val tmStmp = if (insertTimeStamp) System.currentTimeMillis() else 0
	   val normTag = "N"
	   val outlierTag = "O"
	   val indeterTag = "I"
	   val totalTag = "T"
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key by record key and record status
	   val keyedCounters = data.flatMap(line => {
   		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
   		   val counters = for (i <- 0 to 1) yield {
   			   val keyRec = Record(keyLen+1, items, 0, keyLen)
   			   if (i == 0) keyRec.addString(items(items.length-1))
   			   else keyRec.addString(totalTag)
   			   (keyRec, 1)
   		   }
   		   counters
	   }).reduceByKey((v1,v2) => v1+v2)
	   
	   //formatted count statistics for each key
	   val formattedCountRecs = keyedCounters.map(r => {
	     val keyRec = Record(r._1, 0, keyLen)
	     val valRec = Record(2)
	     valRec.addString(r._1.getString(keyLen))
	     valRec.addInt(r._2)
	     (keyRec, valRec)
	   }).groupByKey().map(r => {
	     val key = r._1
	     val values = r._2.toArray
	     var outlierCount = 0
	     var indeterCount = 0
	     var normCount = 0
	     var totalCount = 0
	     for (v <- values) {
	    	 v.getString(0) match {
	    	   case `outlierTag` => outlierCount = v.getInt(1)
	    	   case `indeterTag` => indeterCount = v.getInt(1)
	    	   case `normTag` => normCount = v.getInt(1)
	    	   case `totalTag` => totalCount = v.getInt(1)
	    	 }
	     }
	     val outlierPercent = (outlierCount * 100).toDouble / totalCount
	     val indeterPercent = (indeterCount * 100).toDouble / totalCount
	     val normPercent = (normCount * 100).toDouble / totalCount
	     
	     val stBld = new StringBuilder(key.toString())
	     if (insertTimeStamp)
	       stBld.append(fieldDelimOut).append(tmStmp)
	     stBld.
	       	append(fieldDelimOut).append(outlierCount).
	     	append(fieldDelimOut).append(BasicUtils.formatDouble(outlierPercent, precision)).
	     	append(fieldDelimOut).append(indeterCount).
	     	append(fieldDelimOut).append(BasicUtils.formatDouble(indeterPercent, precision)).
	     	append(fieldDelimOut).append(normCount).
	     	append(fieldDelimOut).append(BasicUtils.formatDouble(normPercent, precision)).
	     	append(fieldDelimOut).append(totalCount)
	     
	     stBld.toString()
	   })
	   
       if (debugOn) {
         val records = formattedCountRecs.collect.slice(0, 20)
         records.foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     formattedCountRecs.saveAsTextFile(outputPath) 
	   }
   }

}