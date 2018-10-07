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
	   val splittingAlgo = this.getStringParamOrElse(appConfig, "splitting.algo", "entropy")
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key by split
	   var splitRecs = data.flatMap(line => {
	     val items = line.split(fieldDelimIn, -1)
	     val key = Record(2)
	     val recs = splitPoints.map(sp => {
	       val score = line(scoreFldOrd).toDouble
	       val clLabel = line(clsFldOrd)
	       val part = if (score < sp) 0 else 1
	       val key = Record(2)
	       key.add(sp)
	       key.add(part)
	       
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
	     value.add(v1.getInt(0) + v2.getInt(0))
	     value.add(v1.getInt(1) + v2.getInt(1))
	     value
	   })
	   
	   //calculate info content
	   splitRecs = splitRecs.mapValues(v => {
	     val count = v.getInt(0) + v.getInt(1)
	     val p0 = v.getInt(0).toDouble / count
	     val p1 = v.getInt(1).toDouble / count
	     val info = if (splittingAlgo.equals("entropy")) {
	       -(p0 * Math.log(p0) + p1 * Math.log(p1))
	     } else {
	       1.0 - p0 * p0 - p1 * p1
	     }
	     val value = Record(2)
	     value.addDouble(info)
	     value.addInt(count)
	     value
	   })
	   
	   //weighted average of info content
	   val splitInfo  = splitRecs.map(r => {
	     val key = r._1
	     val value = r._2
	     val split = key.getDouble(0)
	     val part = key.getInt(2)
	     (split, value)
	   }).groupByKey.map(r => {
	     val split = r._1
	     val partInfo = r._2.toList
	     val count = (partInfo(0).getInt(1) + partInfo(1).getInt(1)).toDouble
	     val w0 = partInfo(0).getInt(1) / count
	     val w1 = partInfo(1).getInt(1) / count
	     val info = w0 * partInfo(0).getDouble(0) + w1 * partInfo(1).getDouble(0)
	     BasicUtils.formatDouble(split, 3) + fieldDelimOut + BasicUtils.formatDouble(info, 6)
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