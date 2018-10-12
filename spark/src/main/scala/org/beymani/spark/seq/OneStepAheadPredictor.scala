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

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import org.chombo.util.BaseAttribute
import com.typesafe.config.Config
import org.hoidla.window.SizeBoundPredictorWindow
import org.chombo.stats.SimpleStat
import org.chombo.util.MathUtils
import org.beymani.spark.common.OutlierUtility

/**
 * Anomaly detection in sequence data based on one step ahead prediction
 * @author pranab
 *
 */
object OneStepAheadPredictor extends JobConfiguration with OutlierUtility {
  
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
	   val predictorStrategy = getStringParamOrElse(appConfig, "predictor.strategy", 
	       SizeBoundPredictorWindow.PRED_AVERAGE)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   
	  val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	  val attrOrdsList = attrOrds.toList
	  val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	  val outputOutliers = getBooleanParamOrElse(appConfig, "output.outliers", false)
	  val remOutliers = getBooleanParamOrElse(appConfig, "rem.outliers", false)
	  val cleanDataDirPath = getConditionalMandatoryStringParam(remOutliers, appConfig, "clean.dataDirPath", 
	       "missing clean data file output directory")
	  val statDataDirPath = getMandatoryStringParam(appConfig, "stat.dataDirPath", 
	       "missing stat file output directory path")
	  val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")	   
	  val thresholdNorm = getOptionalDoubleParam(appConfig, "score.thresholdNorm")
	  val expConst = getDoubleParamOrElse(appConfig, "exp.const", 1.0)	 
	  val attWeightList = getMandatoryDoubleListParam(appConfig, "attr.weights", "missing attribute weights")
	  val attrWeights = BasicUtils.fromListToDoubleArray(attWeightList)
	  val windowSize = getIntParamOrElse(appConfig, "window.size", 3)
	  val minStatCount = getIntParamOrElse(appConfig, "min.statCount", 5)
	  val rangeConfLevel = getDoubleParamOrElse(appConfig, "range.confLevel", 0.95)
	  val tDistVal = MathUtils.linearInterpolate(MathUtils.tDistr, rangeConfLevel)
	  val statTag = "$STAT$"
	  val averagingWeights = if (predictorStrategy.equals(SizeBoundPredictorWindow.PRED_WEIGHTED_AVERAGE)) {
	     val wtList = getMandatoryDoubleListParam(appConfig, "averaging.weights", "missing averaging weights")
	     BasicUtils.fromListToDoubleArray(wtList)
	  } else {
	     null
	  }
	   
	  val expSmoothFactor = if (predictorStrategy.equals(SizeBoundPredictorWindow.PRED_EXP_SMOOTHING)) {
	     getMandatoryDoubleParam(appConfig, "exp.smoothFactor", "missing exponential smoothing factor")
	   } else {
	     0
	  }
	  
	  //residue stats
	  val resStatFilePath = getMandatoryStringParam(appConfig, "res.statFilePath", "missing residute stats file")
	  val resStats = getResidueStats(resStatFilePath, fieldDelimIn)
	  val brResStats = sparkCntxt.broadcast(resStats)
	  
	  val debugOn = appConfig.getBoolean("debug.on")
	  val saveOutput = appConfig.getBoolean("save.output")
	  
	  //for sorting by sequence
	  val sortFields = Array[Int](1)
	  sortFields(0) = 0

	  var keyLen = 0
	  keyFieldOrdinals match {
	    case Some(fields : Array[Integer]) => keyLen +=  fields.length
	    case None =>
	  }
	   
	 //input
	 val data = sparkCntxt.textFile(inputPath)
	 if (remOutliers)
	   data.cache
	   
	 val keyedData = data.map(line => {
	   val items = line.split(fieldDelimIn, -1)
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
	   
	   val value = Record(2)
	   val seq = items(seqFieldOrd).toLong
	   value.addLong(seq)
	   value.addString(line)
	   (key, value)
	 })	   
	   
	 val allTaggedData = keyedData.groupByKey.flatMap(v => {
	   val key = v._1
	   val keyStr = key.toString
	   val values = v._2.toList.sortBy(v => v.getLong(0))
	   
	   //window
	   var windows = Map[Int, SizeBoundPredictorWindow]()
	   attrOrdsList.foreach(i => {
	     windows += (i -> new SizeBoundPredictorWindow(windowSize, predictorStrategy))
	   })
	   
	   //residue stats
	   val allResStats = brResStats.value
	   var resStats = Map[Int,SimpleStat]()
	   attrOrdsList.foreach(i => {
	     val statsKey = key.toString + fieldDelimIn + i
	     val stat = allResStats.getOrElse(statsKey, null)
	     val clonedStat = new SimpleStat(stat.getCount(), stat.getSum(), stat.getSumSq(),
	         stat.getMean(), stat.getStdDev());
	     resStats += (i -> clonedStat)
	   })
	   
	   //tagged records
	   val recs = values.map(v => {
	     val line = v.getString(1)
	     val items = line.split(fieldDelimIn, -1)
	     
	     val scores = attrOrdsList.map(i => {
	       val window = windows.getOrElse(i, null)
	       val stat = resStats.getOrElse(i, null)
	       val quant = items(i).toDouble
	       window.add(quant)
	       val quantPrediction = window.getPrediction()
	       val score = if (stat.getCount() > minStatCount) {
	         val count = stat.getCount()
	         val stdDev = stat.getStdDev()
	         val range = tDistVal * stdDev * Math.sqrt(1.0 + 1.0 / count)
	         var score = Math.abs(quantPrediction - quant) / range
	         score = BasicUtils.expScale(expConst, score);
	         
	         //if outlier replace actual with predicted in window
	         if (score > scoreThreshold)
	        	 window.replaceRecent(quantPrediction)
	         score
	       } else {
	         0.0
	       }
	       stat.add(quant - quantPrediction)
	       score
	     }).toArray
	     
	     val score = MathUtils.weightedAverage(scores, attrWeights)
	     val marker = if (score > scoreThreshold) "O"  else "N"
	     line + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + marker  
	   })
	   
	   //records for stat
	   val statRecs = resStats.map(v => {
	     statTag + keyStr + fieldDelimOut + v._1.toString + fieldDelimOut + v._2.toString
	   }).toList
	   
	   recs ++ statRecs
	 }).cache
	 
	 //normal records
	 var taggedData = allTaggedData.filter(line => {
	   !line.startsWith(statTag)
	 })
	   
	 //stat records
	 val statData = allTaggedData.
	 	filter(line => line.startsWith(statTag)).
	 	map(line => line.substring(statTag.length()))
	 statData.saveAsTextFile(statDataDirPath) 
	 
	 //process tagged records
	 taggedData = processTaggedData(outputOutliers, remOutliers, cleanDataDirPath,fieldDelimIn, fieldDelimOut, 
	     thresholdNorm, taggedData, data)	 	

	 if (debugOn) {
         val records = taggedData.collect
         records.slice(0, 100).foreach(r => println(r))
     }
	   
	 if(saveOutput) {	   
	     taggedData.saveAsTextFile(outputPath) 
	 }	 
	     
   }
   
   /**
   * @param filePath
   * @param fieldDelimIn
   * @return
   */
   def getResidueStats(filePath:String, fieldDelimIn:String) : Map[String, SimpleStat]= {
     var stats = Map[String, SimpleStat]()
     val lines = BasicUtils.getFileLines(filePath).asScala.toList
     var pos = -1
     lines.foreach(line => {
       if (pos < 0){
    	   val items = line.split(fieldDelimIn, -1)
    	   pos = BasicUtils.findOccurencePosition(line, fieldDelimIn, items.length - 5, true)
       }
       val parts = BasicUtils.splitOnPosition(line, pos, 1,  true)
       val key = parts(0)
       val stat = new SimpleStat()
       stat.fromString(parts(1))
       stats += (key -> stat)
     })
     stats
   }
   
   
}