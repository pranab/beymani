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

object OneStepAheadPredictor extends JobConfiguration {
   private val predStrategyAverage = "average";
   private val predStrategyWeightedAverage = "weightedAverage";
   private val predStrategyMedian = "median";
   private val predStrategyRegression = "regression";
   private val predStrategyExponential = "exponential";
   private val predStrategyHoltLinear = "holtLinear";
  
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
	   val predictorStrategy = getStringParamOrElse(appConfig, "predictor.strategy", predStrategyAverage)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val outputOutliers = getBooleanParamOrElse(appConfig, "output.outliers", false)
	   val remOutliers = getBooleanParamOrElse(appConfig, "rem.outliers", false)
	   val cleanDataDirPath = getConditionalMandatoryStringParam(remOutliers, appConfig, "clean.dataDirPath", 
	       "missing clean data file output directory")
	   val threshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")	   
	   val expConst :java.lang.Double = getDoubleParamOrElse(appConfig, "exp.const", 1.0)	 
	   val attWeightList = getMandatoryDoubleListParam(appConfig, "attr.weights", "missing attribute weights")
	   val attrWeights = BasicUtils.fromListToDoubleArray(attWeightList)
	   val windowSize = getIntParamOrElse(appConfig, "window.size", 3)
	   
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
	   
	 val taggedData = keyedData.groupByKey.flatMap(v => {
	   var windows = Map[Int, SizeBoundPredictorWindow]()
	   attrOrds.toList.foreach(i => {
	     windows += (i -> new SizeBoundPredictorWindow(windowSize, predictorStrategy))
	   })
	   val key = v._1
	   var values = v._2.toList
	   values = values.sortBy(v => v.getLong(0))
	   values.map(v => {
	     val line = v.getString(1)
	     val items = line.split(fieldDelimIn, -1)
	     
	     attrOrds.toList.foreach(i => {
	       val windowOpt = windows.get(i)
	       val quant = items(i).toDouble
	       windowOpt match {
	         case Some(window:SizeBoundPredictorWindow) => {
	           window.add(quant)
	           
	         }
	         case None => throw new IllegalStateException("")
	       }
	     })
	   })
	   
	   List[Record]()
	 })
	   
	   
   }
}