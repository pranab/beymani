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
import org.beymani.spark.common.OutlierUtility
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.SeasonalUtility
import org.beymani.predictor.MarkovModelPredictor


object MarkovChainPredictor extends JobConfiguration with OutlierUtility  with GeneralUtility with SeasonalUtility {
  
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "markovChainPredictor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val predictorStrategy = getStringParamOrElse(appConfig, "predictor.strategy", "conditinalProbability")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val keyFields = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val scoreThreshold:java.lang.Double = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")
	   val attrOrd = getMandatoryIntParam(appConfig, "attr.ordinal")
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val outputOutliers = getBooleanParamOrElse(appConfig, "output.outliers", false)
	   val seasonalTypeFldOrd = getOptionalIntParam(appConfig, "seasonal.typeFldOrd")
	   val seasonalTypeInData = seasonalTypeFldOrd match {
		     case Some(seasonalOrd:Int) => true
		     case None => false
	   }
	   val windowSize = getIntParamOrElse(appConfig, "window.size", 3)
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val analyzerMap = creatSeasonalAnalyzerMap(this, appConfig, seasonalAnalysis, seasonalTypeInData)
	   val analyzers = creatSeasonalAnalyzerArray(this, appConfig, seasonalAnalysis, seasonalTypeInData)
	   val states = getMandatoryStringListParam(appConfig, "state.list", "")
	   val stateTransFilePath = getMandatoryStringParam(appConfig, "stateTrans.filePath", "missing state transition file path")
	   val stateTransCompact = getBooleanParamOrElse(appConfig, "stateTrans.compact", true)
	   val fileLines = BasicUtils.getFileLines(stateTransFilePath)
	   val expConst :java.lang.Double = getDoubleParamOrElse(appConfig, "exp.const", 1.0)
	   val globalModel = getBooleanParamOrElse(appConfig, "model.global", false)
	   val ignoreMissingModel = getBooleanParamOrElse(appConfig, "ignore.missingModel", false)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val markovPredictors = MarkovModelPredictor.createKeyedMarkovModel(true, fileLines, stateTransCompact, fieldDelimIn, states, 
	       predictorStrategy, windowSize, attrOrd, expConst)
	   for (key  <- markovPredictors.keySet().asScala) {
	     markovPredictors.get(key).withEnqueScore(false).withDebugOn(debugOn)
	   }
	   val keyLen = getKeyLength(keyFields, seasonalAnalysis) 

	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   	   
	   val taggedData = data.map(line => {
		 val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		 val key = Record(keyLen)
		 addPrimarykeys(items, keyFields,  key)
		 addSeasonalKeys(this, appConfig,analyzerMap, analyzers, items, seasonalAnalysis, key)
	     val value = Record(2)
	     value.addLong(items(seqFieldOrd).toLong)
	     value.addString(line)
	   	 (key, value)
	   }).groupByKey.flatMap(r => {
	     val key = r._1
	     val keyStr = key.toString()
	     val mKey = getModelKey(key, seasonalAnalysis, globalModel).toString
	     val predictor = markovPredictors.get(mKey)
	     val values = r._2.toArray.sortBy(v => v.getLong(0))
	     val scoreTags = values.map(r => {
	       val rec = r.getString(1)
	       var score = -1.0
	       var tag = "I"
	       if (null != predictor) {
	    	   score = predictor.execute(keyStr, rec)
	    	   tag = if (score > scoreThreshold) "O" else "N"
	       }
	       val scoreTag = Record(2)
	       scoreTag.add(score, tag)
	       scoreTag
	     })
	     
	     //spread outlier tag over window
	     for (i <- 0 to scoreTags.length -1) {
	       val scoreTag = scoreTags(i)
	       if (scoreTag.getString(1).equals("O")) {
	         for (j <- i - windowSize + 1 to i - 1) {
	           scoreTags(j).addString(1, scoreTag.getString(1))
	         }
	       }
	     }
	     
	     //add tag and score
	     scoreTags.zip(values).map(r => {
	       val score = r._1.getDouble(0)
	       val tag = r._1.getString(1)
	       val rec = r._2.getString(1)
	       rec + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + tag 
	     })
	   })
	   
	 if (debugOn) {
         val records = taggedData.collect
         records.slice(0, 100).foreach(r => println(r))
     }
	   
	 if(saveOutput) {	   
	     taggedData.saveAsTextFile(outputPath) 
	 }	 
	   
   }
}