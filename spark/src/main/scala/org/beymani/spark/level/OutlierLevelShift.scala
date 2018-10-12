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

package org.beymani.spark.level

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import org.chombo.util.BaseAttribute
import com.typesafe.config.Config
import org.chombo.stats.NumericalAttrStatsManager
import org.hoidla.window.OutlierBasedLevelShiftDetector
import org.hoidla.util.TimeStampedTaggedDouble
import scala.collection.mutable.ArrayBuffer
import org.chombo.util.Pair

object OutlierLevelShift extends JobConfiguration  {
  
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "oneStepAheadPredictor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val windowSize = getMandatoryIntParam(appConfig, "window.size", "missing window size")
	   val maxTolerance = getMandatoryIntParam(appConfig, "max.tolerance", "missing tolerance level")
	   val outlierLabel = getStringParamOrElse(appConfig, "outlier.label", "O")
	   val outlierLabelOrd = this.getMandatoryIntParam(appConfig, "outlier.labelOrd", "missing ourtlier label field ordinal")
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val statsManager = getStatsManager(appConfig)
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	   var keyLen = 0
	   keyFieldOrdinals match {
	     case Some(fields : Array[Integer]) => keyLen +=  fields.length
	     case None =>
	   }
	   keyLen += 1
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key by id and quant field ordinals
	   var keyeddData = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   
		   val recs = attrOrds.map(a => {
			   val key = Record(keyLen)
	
			   //partitioning fields
			   keyFieldOrdinals match {
		           case Some(fields : Array[Integer]) => {
		             for (kf <- fields) {
		               key.addString(items(kf))
		             }
		           }
		           case None =>
		       }
			   key.addInt(a)
			   
			   val value = Record(3)
			   value.addLong(items(seqFieldOrd).toLong)
			   value.addDouble(items(a).toDouble)
			   value.addString(items(outlierLabelOrd))
			   (key, value)
		   })
		   recs
	   })
	   
	   //detect level shift
	   val levelShits = keyeddData.groupByKey.flatMap(r => {
	     val key = r._1
	     val size = key.size
	     val mean = if (size == 1) {
	       val attr = key.getInt(0)
	       statsManager.getMean(attr)
	     }else {
	       val compKey = key.toString(0, size-1)
	       val attr = key.getInt(size-1)
	       statsManager.getMean(compKey, attr)
	     }
	     val window = new OutlierBasedLevelShiftDetector(windowSize)
	     window.withOutlierLabel(outlierLabel)
	     window.withMean(mean)
	     
	     val values = r._2.toList.sortBy(v => v.getLong(0))
	     val levelShitfs = ArrayBuffer[Record]()    	     
	     var segment  = new Record(2)
	     values.foreach(v => {
	       val tagged = new TimeStampedTaggedDouble(v.getDouble(1), v.getLong(0), v.getString(2))
	       window.add(tagged)
	       if (window.isLevelShifted()) {
	         if (window.isNewLevelShift()) {
	           val start = window.getLevelShiftStart()
	           val end = window.getLevelShiftEnd()
	           segment  = new Record(2)
	           segment.add(start, end)
	           levelShitfs += segment
	         } else {
	           val end = window.getLevelShiftEnd()
	           segment.addLong(1, end)
	         }
	       }
	     })
	     levelShitfs.toList.map(v => (key, v))
	   })
	   
	   //sort
	   val sortedLevelShits = levelShits.map(r => {
	     val key = r._1
	     val value = r._2
	     val newKey = Record(key.size+1, key)
	     newKey.addLong(value.getLong(0))
	     (newKey, value)
	   }).sortByKey().map(r => {
	     r._1.toString(0, r._1.size -1) + fieldDelimOut + r._2.toString()
	   })
	   
	 if (debugOn) {
         val records = sortedLevelShits.collect
         records.slice(0, 100).foreach(r => println(r))
     }
	   
	 if(saveOutput) {	   
	     sortedLevelShits.saveAsTextFile(outputPath) 
	 }	 
	   
   }
   
   /**
   * @param appConfig
   * @return
   */
   def getStatsManager(appConfig : Config) : NumericalAttrStatsManager = {
     val statsFilePath = getMandatoryStringParam(appConfig, "stats.file.path", "missing stat file path")
     val isHdfsFile = getBooleanParamOrElse(appConfig, "hdfs.file", false)
     val partIdOrds = getOptionalIntListParam(appConfig, "id.fieldOrdinals");
	 val idOrdinals = partIdOrds match {
	     case Some(idOrdinals: java.util.List[Integer]) => BasicUtils.fromListToIntArray(idOrdinals)
	     case None => null
	 }
     val statsManager =  new NumericalAttrStatsManager(statsFilePath, ",",idOrdinals,  false, isHdfsFile)
     statsManager
   }
}