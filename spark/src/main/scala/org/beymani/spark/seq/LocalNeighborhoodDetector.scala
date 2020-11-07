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

package org.beymani.spark.seq

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.control.Breaks._
import org.apache.spark.SparkContext
import org.beymani.spark.common.OutlierUtility
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.chombo.math.MathUtils
import org.beymani.util.SeequenceScoreAggregator
import org.hoidla.window.LocalNeighborhoodWindow


/**
 * Anomaly detection in sequence data based on nearest neighboers  within an window.
 * @author pranab
 *
 */
object LocalNeighborhoodDetector extends JobConfiguration with GeneralUtility with OutlierUtility {
  
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "localNeighborhoodDetector"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val attrOrd = getMandatoryIntParam(appConfig, "attr.ordinal")
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")	
	   val windowSize = getIntParamOrElse(appConfig, "window.size", 3)
	   val neighborhoodDist = getDoubleParamOrElse(appConfig, "neighborhood.dist", -1.0)
     val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
		 
	   BasicUtils.assertCondition(windowSize % 2 == 1, "window size should be odd")
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val neighborhoodDistBased = neighborhoodDist > 0
	   val neighborhoodSize = getConditionalMandatoryIntParam(!neighborhoodDistBased, appConfig, "neighborhood.size", 
	       "neighborhoosd size must be provided")
	   
	   //input
		 var  sameRefData = false
	   val data = sparkCntxt.textFile(inputPath)
	   val keyedData = getKeyedValueWithSeq(data, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd)
		 
	   //records with tag and score
	   val taggedData = keyedData.groupByKey.flatMap(v => {
       val key = v._1
	     val values = v._2.toList.sortBy(v => v.getLong(0))
	     val size = values.length
	     val coffset = size / 2 - 1
	     val window = if (neighborhoodDistBased) {
         new LocalNeighborhoodWindow(windowSize, neighborhoodDist)
	     } else {
         new LocalNeighborhoodWindow(windowSize, neighborhoodSize)
       }
       val scores = Array.fill[Double](size)(0)
	     for (i <- 0 to size - 1) {
	       val v = values(i)
	       val line = v.getString(1)
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	       val quant = items(attrOrd).toDouble
	       window.add(quant)
	       if (window.isProcessed()) {
	         val score = if (neighborhoodDistBased) window.getNumNeighbosWithin().toDouble
	           else  window.getAvNeighborDist()
	         scores(i - coffset) = score 
	       }
	     }
       
       //append score and tag
       val recScores = values.map(r => r.getString(1)).zip(scores)
       recScores.map(r => {
         val rec = r._1
         val score = r._2
         val tag = if (score > scoreThreshold) "O" else "N"
         rec + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + tag
       })
	   })

	   if (debugOn) {
       val records = taggedData.collect
       records.slice(0, 50).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     taggedData.saveAsTextFile(outputPath) 
	   }	 
	   
   }
  
}