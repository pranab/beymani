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
* Decision stump based alarm threshold learner based on user feedback 
* @param args
* @return
*/
object ThresholdLearner extends JobConfiguration {
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "thresholdLearner"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val scoreFldOrd = getMandatoryIntParam(appConfig, "score.fldOrd", "missing score field ordinal")
	   val clsFldOrd = getMandatoryIntParam(appConfig, "cls.fldOrd", "missing class label field ordinal")
	   val splitPoints = getMandatoryDoubleListParam(appConfig, "split.points", "missing split points").asScala.toList
	   val posClsLabel = getStringParamOrElse(appConfig, "pos.clsLabel", "T")
	   val splittingAlgo = getStringParamOrElse(appConfig, "splitting.algo", "entropy")
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   var keyLen = 0
	   keyFieldOrdinals match {
	     case Some(fields : Array[Integer]) => keyLen +=  fields.length
	     case None =>
	   }
	   keyLen += 2
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key by split
	   var splitRecs = data.flatMap(line => {
	     val items = line.split(fieldDelimIn, -1)
	     val score = items(scoreFldOrd).toDouble
	     val clLabel = items(clsFldOrd)
	     val recs = splitPoints.map(sp => {
	       val part = if (score < sp) 0 else 1
	       val key = Record(keyLen)
	       Record.populateFields(items, keyFieldOrdinals, key)
	       key.addDouble(sp)
	       key.addInt(part)
	    	   
	       val value = Record(2)
	       if (clLabel.equals(posClsLabel)) {
	         value.addInt(1)
	         value.addInt(0)
	       } else {
	         value.addInt(0)
	         value.addInt(1)
	       }
	       (key, value)
	     })
	     recs
	   })
	   
	   //aggregate counts
	   splitRecs = splitRecs.reduceByKey((v1, v2) => {
	     val value = Record(2)
	     value.addInt(v1.getInt(0) + v2.getInt(0))
	     value.addInt(v1.getInt(1) + v2.getInt(1))
	     value
	   })
	   
	   //calculate info content
	   splitRecs = splitRecs.mapValues(v => {
	     val count0 = v.getInt(0)
	     val count1 = v.getInt(1)
	     val count = count0 + count1
	     val p0 = count0.toDouble / count
	     val p1 = count1.toDouble / count
	     val info = if (splittingAlgo.equals("entropy")) {
	       var info = 0.0
	       if (count0 > 0) info -=  p0 * Math.log(p0)
	       if (count1 > 0) info -=  p1 * Math.log(p1)
	       info
	     } else {
	       1.0 - p0 * p0 - p1 * p1
	     }
	     if (debugOn)
	    	 println("count " + count0 + " count " + count1 + " info " + info)
	     val value = Record(2)
	     value.addDouble(info)
	     value.addInt(count)
	     value
	   })
	   
	   //weighted average of info content
	   val splitInfo  = splitRecs.map(r => {
	     val key = r._1
	     val value = r._2
	     val newKey = Record(key, 0, key.size-1)
	     (newKey, value)
	   }).groupByKey.map(r => {
	     val key = r._1
	     val partInfo = r._2.toArray
	     if (partInfo.size != 2) {
	       throw new IllegalStateException("num of partions is not 2")
	     }
	     val count = (partInfo(0).getInt(1) + partInfo(1).getInt(1)).toDouble
	     val w0 = partInfo(0).getInt(1) / count
	     val w1 = partInfo(1).getInt(1) / count
	     val info0 = partInfo(0).getDouble(0)
	     val info1 = partInfo(1).getDouble(0)
	     val info = w0 * info0 + w1 * info1
	     if (debugOn)
	    	 println("count " + count + " w0 " + w0 + " w1 " + w1 + " info0 " + info0 + " info1 " + info1)
	     key.toString + fieldDelimOut + BasicUtils.formatDouble(info, 6)
	   })
	   
	   if (debugOn) {
         val records = splitInfo.collect
         records.foreach(r => println(r))
	   }
	   
	   if(saveOutput) {	   
	     splitInfo.saveAsTextFile(outputPath) 
	   }	 
   }
}