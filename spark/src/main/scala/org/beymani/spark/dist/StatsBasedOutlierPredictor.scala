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
package org.beymani.spark.dist

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import org.chombo.util.BaseAttribute
import com.typesafe.config.Config
import java.lang.Boolean
import org.beymani.predictor.ZscorePredictor
import org.beymani.predictor.RobustZscorePredictor
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.beymani.predictor.EstimatedProbabilityBasedPredictor

object StatsBasedOutlierPredictor extends JobConfiguration with SeasonalUtility {
   private val predStrategyZscore = "zscore";
   private val predStrategyRobustZscore = "robustZscore";
   private val predStrategyEstProb = "estimatedProbablity";
   private val predStrategyEstAttrProb = "estimatedAttributeProbablity";
   
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "statsBasedOutlierPredictor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val predictorStrategy = getStringParamOrElse(appConfig, "predictor.strategy", predStrategyZscore)
	   val appAlgoConfig = appConfig.getConfig(predictorStrategy)
	   val algoConfig = getConfig(predictorStrategy, appConfig, appAlgoConfig)
	   val scoreThreshold:java.lang.Double = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   val outputOutliers = getBooleanParamOrElse(appConfig, "output.outliers", false)
	   val remOutliers = getBooleanParamOrElse(appConfig, "rem.outliers", false)
	   val cleanDataDirPath = getConditionalMandatoryStringParam(remOutliers, appConfig, "clean.dataDirPath", 
	       "missing clean data file output directory")
	   
	   
	   //seasonal data
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val partBySeasonCycle = getBooleanParamOrElse(appConfig, "part.bySeasonCycle", true)
	   val seasonalAnalyzers = if (seasonalAnalysis) {
		   val seasonalCycleTypes = getMandatoryStringListParam(appConfig, "seasonal.cycleType", 
	        "missing seasonal cycle type").asScala.toArray
	        val timeZoneShiftHours = getIntParamOrElse(appConfig, "time.zoneShiftHours", 0)
	        val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	        "missing time stamp field ordinal")
	        val timeStampInMili = getBooleanParamOrElse(appConfig, "time.inMili", true)
	        
	        val analyzers = seasonalCycleTypes.map(sType => {
	    	val seasonalAnalyzer = createSeasonalAnalyzer(this, appConfig, sType, timeZoneShiftHours, timeStampInMili)
	        seasonalAnalyzer
	    })
	    Some((analyzers, timeStampFieldOrdinal))
	   } else {
		   None
	   }
	   
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	   val predictor = predictorStrategy match {
       	case `predStrategyZscore` => new ZscorePredictor(algoConfig, "id.fieldOrdinals", "attr.ordinals", 
       	    "field.delim.in", "attr.weights", "stats.filePath", "seasonal.analysis", "hdfs.file", "score.threshold");
         
       	case `predStrategyRobustZscore` => new RobustZscorePredictor(algoConfig, "id.fieldOrdinals", "attr.ordinals", 
       	    "field.delim.in", "attr.weights", "stats.medFilePath", "stats.madFilePath", "seasonal.analysis","hdfs.file", "score.threshold");
     
       	case `predStrategyEstProb` => new EstimatedProbabilityBasedPredictor(algoConfig, "id.fieldOrdinals", 
       	     "distr.file.path", "hdfs.file", "schema.file.path", "score.threshold")
	   }
	   
	   
	 //broadcast validator
	 val brPredictor = sparkCntxt.broadcast(predictor)
	   
	 //input
	 val data = sparkCntxt.textFile(inputPath)
	 if (remOutliers)
	   data.cache
	   
	 //predict for each field in each line whether it's an outlier
	  var keyLen = 0
	  keyFieldOrdinals match {
	    case Some(fields : Array[Integer]) => keyLen +=  fields.length
	    case None =>
	  }
	  keyLen += (if (seasonalAnalysis) 1 else 0)
	  keyLen += (if (seasonalAnalysis && partBySeasonCycle) 1 else 0)

	 var taggedData = data.map(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val key = Record(keyLen)
		   //partioning fields
		   keyFieldOrdinals match {
	           case Some(fields : Array[Integer]) => {
	             for (kf <- fields) {
	               key.addString(items(kf))
	             }
	           }
	           case None =>
	       }
		     
		   //seasonality cycle
		   seasonalAnalyzers match {
		     case Some(seAnalyzers : (Array[SeasonalAnalyzer], Int)) => {
		         val timeStamp = items(seAnalyzers._2).toLong
		         val cIndex = SeasonalAnalyzer.getCycleIndex(seAnalyzers._1, timeStamp)
		         key.addString(cIndex.getLeft())
		         if (partBySeasonCycle) key.addInt(cIndex.getRight())
		       }
		     case None => 
		   }	  
		   val keyStr = key.toString
		   val predictor = brPredictor.value
		   val score:java.lang.Double = predictor.execute(items, keyStr)
		   val marker = if (score > scoreThreshold) "O"  else "N"
		   line + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + marker
	 })
	 
	 if (outputOutliers || remOutliers) {
	   taggedData = taggedData.filter(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val marker = items(items.length - 1)
		   marker.equals("O")
	   })
	   if (remOutliers) {
	     taggedData = taggedData.map(line => {
		   val items = line.split(fieldDelimIn, -1)
	       val ar = items.slice(0, items.length - 2)
	       ar.mkString(fieldDelimOut)
	     })
	     
	     //remove outliers records
	     val cleanData =  data.subtract(taggedData)
	     cleanData.saveAsTextFile(cleanDataDirPath) 
	   }
	 } 
	  
	 if (debugOn) {
         val records = taggedData.collect
         records.slice(0, 100).foreach(r => println(r))
     }
	   
	 if(saveOutput) {	   
	     taggedData.saveAsTextFile(outputPath) 
	 }	 
	 
   }
   
      /**
   * @param args
   * @return
   */
   def getConfig(predictorStrategy : String, appConfig : Config,  appAlgoConfig : Config) : java.util.Map[String, Object] = {
	   val configParams = new java.util.HashMap[String, Object]()
	   val partIdOrds = getOptionalIntListParam(appConfig, "id.fieldOrdinals");
	   val idOrdinals = partIdOrds match {
	     case Some(idOrdinals: java.util.List[Integer]) => BasicUtils.fromListToIntArray(idOrdinals)
	     case None => null
	   }
	   configParams.put("id.fieldOrdinals", idOrdinals)
	   
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   configParams.put("attr.ordinals", attrOrds)
	   
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   configParams.put("field.delim.in", fieldDelimIn)

	   val scoreThreshold:java.lang.Double = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")
	   configParams.put("score.threshold", scoreThreshold);
	   
	   val seasonalAnalysis:java.lang.Boolean = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   configParams.put("seasonal.analysis", seasonalAnalysis);
	   
	   predictorStrategy match {
	     case `predStrategyZscore` => {
	       val attWeightList = getMandatoryDoubleListParam(appAlgoConfig, "attr.weights", "missing attribute weights")
	       val attrWeights = BasicUtils.fromListToDoubleArray(attWeightList)
	       configParams.put("attr.weights", attrWeights)
	       val statsFilePath = getMandatoryStringParam(appAlgoConfig, "stats.file.path", "missing stat file path")
	       configParams.put("stats.filePath", statsFilePath)
	       val isHdfsFile = getBooleanParamOrElse(appAlgoConfig, "hdfs.file", false)
	       configParams.put("hdfs.file", new java.lang.Boolean(isHdfsFile))
	     }
	     case `predStrategyRobustZscore` => {
	       val attWeightList = getMandatoryDoubleListParam(appAlgoConfig, "attr.weights", "missing attribute weights")
	       val attrWeights = BasicUtils.fromListToDoubleArray(attWeightList)
	       configParams.put("attr.weights", attrWeights)
	       val medStatsFilePath = getMandatoryStringParam(appAlgoConfig, "stats.medFilePath", "missing med stat file path")
	       configParams.put("stats.medFilePath", medStatsFilePath)
	       val madStatsFilePath = getMandatoryStringParam(appAlgoConfig, "stats.madFilePath", "missing mad stat file path")
	       configParams.put("stats.madFilePath", madStatsFilePath)
	       val isHdfsFile = getBooleanParamOrElse(appAlgoConfig, "hdfs.file", false)
	       configParams.put("hdfs.file", new java.lang.Boolean(isHdfsFile))
	     }
	     case `predStrategyEstProb` => {
	       val distrFilePath = getMandatoryStringParam(appAlgoConfig, "distr.file.path", "missing distr file path")
	       configParams.put("distr.filePath", distrFilePath)
	       val isHdfsFile = getBooleanParamOrElse(appAlgoConfig, "hdfs.file", false)
	       configParams.put("hdfs.file", new java.lang.Boolean(isHdfsFile))
	       val schemaFilePath = getMandatoryStringParam(appAlgoConfig, "schema.file.path", "missing schema file path")
	       configParams.put("schema.filePath", schemaFilePath)
	     }
	     case `predStrategyEstAttrProb` => {
	       
	     }
	   }
	   
	   configParams
   }
   

}