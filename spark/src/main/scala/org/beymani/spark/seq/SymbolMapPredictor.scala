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
import org.beymani.util.SeequenceScoreAggregator


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
	   val ngramFreqFilePath = getMandatoryStringParam(appConfig, "ngram.freq.file.path", "missing distr file path")
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

     //empirical distribution
		 val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
		 val isHdfsFile = getBooleanParamOrElse(appConfig, "hdfs.file", false)
		 val keyedPercentiles = HistogramUtility.createEqProbHist(distrFilePath, isHdfsFile, keyLen, numBins).asScala
	   
		 //reference ngram frequency
	   val ngramKeyLen = keyLen + 1 + ngramSize
		 val refNgramFreq = BasicUtils.getKeyedValues(ngramFreqFilePath, ngramKeyLen, ngramKeyLen).asScala
		 val refKeyedNgramFreq = getRefKeyedNgramFreq(refNgramFreq, ngramSize, fieldDelimIn)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val keyedData = getKeyedValueWithSeq(data, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd)
	   
	   //records with tag and score
	   val taggedData = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val keyArr = keyStr.split(fieldDelimIn)
	     val percentiles = keyedPercentiles.get(keyArr).get
	     val values = v._2.toList.sortBy(v => v.getLong(0))
	     val size = values.length
	     val ngMapRef = refKeyedNgramFreq.get(keyStr).get
	     
	     val window = new NgramCounterWindow(windowSize,ngramSize)
	     val scoreAggr = new SeequenceScoreAggregator(windowSize)
	     var isFullFirstTime = false
	     val ngMap = scala.collection.mutable.Map[Array[String], Double]()
	     values.foreach(v => {
	       val seq = v.getLong(0)
	       val line = v.getString(1)
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	         val quant = items(attrOrd).toDouble
	         val quantSym = HistogramUtility.getPercentileIndex(percentiles, quant).toString()
	         window.add(quantSym)
	         if (window.isProcessed()) {
	           val ngCounts = window.getNgramCounts().asScala
	           if (!isFullFirstTime) {
	             //full count map first time window is full
	             for ((ke, va) <- ngCounts) {
	               ngMap += (ke -> va.toDouble)
	             }
	             isFullFirstTime = true
	           } else {
	             //incremental change, derement and increment count
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
	           val ngMapNorm = maxNormalize(ngMap)
	           val score = findNgramDiff(ngMapNorm, ngMapRef)
	           scoreAggr.add(seq, score)
	         }
	       })
	       
	       val recs = values.map(r => r.getString(1))
	       val scores =  scoreAggr.getScores().asScala
	       val taggedRecs = recs.zip(scores).map(r => {
	         val rec = r._1
	         val score = r._2.getScore()
	         val tag = if (score > scoreThreshold) "O" else "N"
	         rec + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + tag
	       })
         taggedRecs
	   })
	   
	   if (debugOn) {
       val records = taggedData.collect
       records.slice(0, 50).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     taggedData.saveAsTextFile(outputPath) 
	   }	 
	   
   }
  
  /**
 	* @param refNgramFreq
 	* @param ngramSize
 	* @param fieldDelimIn
 	* @return
 	*/
  def getRefKeyedNgramFreq(refNgramFreq:scala.collection.mutable.Map[String, java.lang.Double], ngramSize:Int, fieldDelimIn:String) : 
     scala.collection.mutable.Map[String, scala.collection.mutable.Map[Array[String], Double]] = {
     val refKeyedNgramFreq = scala.collection.mutable.Map[String, scala.collection.mutable.Map[Array[String], Double]]()
     for ((ke, va) <- refNgramFreq) {
       val items = BasicUtils.getTrimmedFields(ke, fieldDelimIn)
       val split = ke.length() - ngramSize
       val key =  items.slice(0, split).mkString(fieldDelimIn)
       val ngram = items.slice(split, ke.length())
       val ngramFreq = refKeyedNgramFreq.getOrElse(key, scala.collection.mutable.Map[Array[String], Double]())
       refKeyedNgramFreq(key) = ngramFreq
       ngramFreq(ngram) = va
     }
     refKeyedNgramFreq
   }
  
  /**
 	* @param ngMap
 	* @param ngMapRef
 	* @return
 	*/
  def findNgramDiff(ngMap:scala.collection.mutable.Map[Array[String], Double], 
      ngMapRef:scala.collection.mutable.Map[Array[String], Double]) : Double = {
    val keys = ngMap.keySet.union(ngMapRef.keySet)
    var sum = 0.0
    keys.foreach(k => {
      val ng = ngMap.getOrElse(k, 0.0)
      val ngRef = ngMapRef.getOrElse(k, 0.0)
      sum += (ng - ngRef) * (ng - ngRef)
    })
    sum
  }
  
  /**
   * max normalizes ngram frequency
 	* @param ngMap
 	* @return
 	*/
  def maxNormalize(ngMap:scala.collection.mutable.Map[Array[String], Double]) : 
    scala.collection.mutable.Map[Array[String], Double] = {
    val maxVal = ngMap.reduce((r1, r2) => {
      if (r1._2 > r2._2) r1 else r2
    })._2
    
    ngMap.map(r => {
      val k = r._1
      val v = r._2 / maxVal
      (k,v)
    })
  }
  
}