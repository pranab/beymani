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

package org.beymani.spark.misc

import org.apache.spark.SparkContext
import org.beymani.spark.common.OutlierUtility
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.chombo.math.MathUtils
import org.beymani.util.OutlierScoreAggregator

/**
 * Anomaly detection based on range i.e outlier if inside range
 * @author pranab
 *
 */
object InRangeBasedPredictor extends JobConfiguration with GeneralUtility with OutlierUtility {
  
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "inRangeBasedPredictor"
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
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val expConst = getDoubleParamOrElse(appConfig, "exp.const", 1.0)	 
	   val aggregationStrategy = getStringParamOrElse(appConfig, "attr.weightStrategy", "weightedAverage")
	   val attWeightList = getMandatoryDoubleListParam(appConfig, "attr.weights", "missing attribute weights")
	   val attrWeights = BasicUtils.fromListToDoubleArray(attWeightList)
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")
	   
	   val rangeGlobal = getBooleanParamOrElse(appConfig, "range.global", true)
	   val globalRangeFilePath = getConditionalMandatoryStringParam(rangeGlobal, appConfig, "range.globalFilePath", 
	       "missing keywise range file path")
	   val globalRange = rangeGlobal match {
	     case true => getGlobalRange(globalRangeFilePath, attrOrds, fieldDelimIn)
	     case false => Array.ofDim[Double](1, 1)
	   }

	   val localRangeFilePath = getConditionalMandatoryStringParam(!rangeGlobal, appConfig, "range.LocalFilePath", 
	       "missing keywise range file path")
	   val keyedLocalRange = rangeGlobal match {
	     case true => scala.collection.immutable.Map[String, (Double, Double, Double)]()
	     case false => getKeyedRange(localRangeFilePath, keyLen, attrOrds, fieldDelimIn)
	   }
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	   //global range outlier score
     val glRangeOutlierScore = (globalRanges:Array[Array[Double]],items: Array[String], attrOrds:Array[Int], 
     attrWeights: Array[Double], expConst:Double) => {
       val attrOrdsIndx = attrOrds.zipWithIndex
       val allScores = globalRanges.map(range => {
         val scores = attrOrdsIndx.map(v => {
  	       val ord = v._1
  	       val indx = v._2
  	       val quant = items(ord).toDouble
  	       val offset = 3 * indx 
  	       val rLo = range(offset)
  	       val rHi = range(offset+1)
  	       val rMid = range(offset+2)
  	       val delta = if (quant > rMid) quant - rHi else rLo - quant
  	       1.0 - MathUtils.logisticScale(expConst, delta)
         })
	       val aggregator = new  OutlierScoreAggregator(attrWeights.length, attrWeights)
	       scores.foreach(s => aggregator.addScore(s))
	       OutlierScoreAggregator.getAggregateScore(aggregator, aggregationStrategy)
       }).toArray
       allScores.max
     }   
	   
     //local range outlier score
     val loRangeOutlierScore = (keyedLocalRange:scala.collection.immutable.Map[String, (Double, Double, Double)],
     items: Array[String], attrOrds:Array[Int], keyStr:String, attrWeights: Array[Double], expConst:Double, 
     fieldDelimIn:String) => {
       val scores = attrOrds.map(ord => {
	       val quant = items(ord).toDouble
	       val extKey = keyStr + fieldDelimIn + ord
	       val range = keyedLocalRange.get(extKey).get
	       val mid = range._3 
	       val delta = if (quant > mid) quant - range._2 else range._1 - quant
	       1.0 - MathUtils.logisticScale(expConst, delta)
       })
	     val aggregator = new  OutlierScoreAggregator(attrWeights.length, attrWeights)
	     scores.foreach(s => aggregator.addScore(s))
	     OutlierScoreAggregator.getAggregateScore(aggregator, aggregationStrategy)
     }
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //keyed data
	   val keyedData =  getKeyedValueWithSeq(data, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd)
	   
	   //records with tag and score
	   val allTaggedData = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val values = v._2.toList.sortBy(v => v.getLong(0))
	     val size = values.length
	     println("before outlier score")
	     
	     values.map(v => {
	       val line = v.getString(1)
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	       val score = rangeGlobal match {
	           case true => glRangeOutlierScore(globalRange, items, attrOrds, attrWeights, expConst)
  	         case false => loRangeOutlierScore(keyedLocalRange, items, attrOrds, keyStr, attrWeights, expConst, fieldDelimIn)
	       }
	       val marker = if (score > scoreThreshold) "O"  else "N"
	       line + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + marker 
	     })
	   })	
	   
	   if (debugOn) {
       val records = allTaggedData.collect
       records.slice(0, 100).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     allTaggedData.saveAsTextFile(outputPath) 
	   }	 
   }
   
   
      /**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
   def getGlobalRange(rangeFilePath:String, attrOrds:Array[Int], fieldDelimIn:String) : 
     Array[Array[Double]] =  {
       val numAttr = attrOrds.length
       val attrOrdsIndx = attrOrds.zipWithIndex
 	     val fileLines = toStringArray(BasicUtils.getFileLines(rangeFilePath))
       val size = fileLines.length
       //val rangeMaitrx = Array.ofDim[Double](size, 3 * numAttr)
	     fileLines.map(line => {
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	       val attrRanges =  attrOrdsIndx.flatMap(v => {
	         val ord = v._1
	         val indx = v._2
	         val rLo = items(indx).toDouble
	         val rHi = items(indx + numAttr).toDouble
	         val rMid = (rLo + rHi) / 2.0
	         val attrRanges = Array.ofDim[Double](3)
	         attrRanges(0) = rLo
	         attrRanges(1) = rHi
	         attrRanges(2) = rMid          
	         attrRanges
         })
         attrRanges
	     })
   }


   /**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
   def getKeyedRange(rangeFilePath:String, keyLen:Int, attrOrds:Array[Int], fieldDelimIn:String) : 
     scala.collection.immutable.Map[String, (Double, Double, Double)] =  {
       val numAttr = attrOrds.length
       val attrOrdsIndx = attrOrds.zipWithIndex
       var keyedRange = scala.collection.immutable.Map[String, (Double, Double, Double)]()
	     val fileLines = BasicUtils.getFileLines(rangeFilePath)
	     fileLines.forEach(line => {
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	       val key = items.slice(0, keyLen).mkString(fieldDelimIn)
	       attrOrdsIndx.foreach(v => {
	         val ord = v._1
	         val indx = v._2
	         val extKey = key + fieldDelimIn + ord
	         
	         val rLo = items(keyLen + indx).toDouble
	         val rHi = items(keyLen + indx + numAttr).toDouble
	         val rMid = (rLo + rHi) / 2.0
	         val rng = (rLo, rHi, rMid)
	         keyedRange += (extKey -> rng)
	       })
	     })
	     keyedRange
   }
 
}