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

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.beymani.spark.common.OutlierUtility
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.chombo.math.MathUtils
import org.hoidla.window.NgramCounterWindow
import org.chombo.stats.HistogramUtility


/**
 * Anomaly detection in sequence data based on quantized symbol ngram frequency.
 * Used equal probability histogram for  quantization. Outputs large scale sequence 
 * outlier. Window size should be set to multiple of dominant cycle in the data.
 * @author pranab
 *
 */
object SymbolMapPredictor extends JobConfiguration with GeneralUtility with OutlierUtility {
  
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "symbolMapPredictor"
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
	   val numBins = getIntParamOrElse(appConfig, "num.bins", 4)
	   val ngramSize = getIntParamOrElse(appConfig, "ngram.size", 2)
	   val thresholdNorm = getOptionalDoubleParam(appConfig, "score.thresholdNorm")
	   val expConst = getDoubleParamOrElse(appConfig, "exp.const", 1.0)	 
	   val attWeightList = getMandatoryDoubleListParam(appConfig, "attr.weights", "missing attribute weights")
	   val attrWeights = BasicUtils.fromListToDoubleArray(attWeightList)
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")	
	   val windowSize = getIntParamOrElse(appConfig, "window.size", 3)
	   val distrFilePath = getMandatoryStringParam(appConfig, "distr.file.path", "missing distr file path")
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

     //empirical distribution
		  val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
		  val isHdfsFile = getBooleanParamOrElse(appConfig, "hdfs.file", false)
		  val keyedPercentiles = HistogramUtility.createEqProbHist(distrFilePath, isHdfsFile, keyLen, numBins).asScala
	   
	   
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val keyedData = getKeyedValueWithSeq(data, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd)
	   
	   	   //records with tag and score
	   val allTaggedData = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val keyArr = keyStr.split(fieldDelimIn)
	     val percentiles = keyedPercentiles.get(keyArr).get
	     val values = v._2.toList.sortBy(v => v.getLong(0))
	     val size = values.length
	     
	     
	     val window = new NgramCounterWindow(windowSize,ngramSize)
	     var isFullFirstTime = false
	     val ngMap = scala.collection.mutable.Map[Array[String], Int]()
	     values.foreach(v => {
	       val line = v.getString(1)
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	         val quant = items(attrOrd).toDouble
	         val quantSym = HistogramUtility.getPercentileIndex(percentiles, quant).toString()
	         window.add(quantSym)
	         if (window.isProcessed()) {
	           val ngCounts = window.getNgramCounts().asScala
	           if (!isFullFirstTime) {
	             //full count map
	             for ((ke, va) <- ngCounts) {
	               ngMap += (ke -> va)
	             }
	             isFullFirstTime = true
	           } else {
	             //derement and increment count
	             for ((ke, va) <- ngCounts) {
	               if (va == -1) {
	                 val count = ngMap.get(ke).get
	                 ngMap(ke) = (count - 1)
	               } else {
	                 val count = ngMap.get(ke)
	                 count match {
	                   case Some(cnt) => ngMap(ke) = (cnt + 1)
	                   case None => ngMap += (ke -> 1)
	                 }
	               }
	             }
	           }
	         }
	       })
	       
	     

	     
	     List()
	   })
   }
  
}