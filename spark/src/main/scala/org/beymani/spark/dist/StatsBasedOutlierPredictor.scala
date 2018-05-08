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


object StatsBasedOutlierPredictor extends JobConfiguration {
   private val predStrategyZscore = "zscore";
   private val predStrategyRobustZscore = "robustZscore";
   private val predStrategyEstProb = "estimatedProbablity";
   private val predStrategyEstAttrProb = "estimatedAttributeProbablity";
   
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "multiVariateDistribution"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val predictorStrategy = getStringParamOrElse(appConfig, "predictor.strategy", predStrategyZscore)
	   
   }
   
      /**
   * @param args
   * @return
   */
   def getConfig(predictorStrategy : String, appConfig : Config,  appAlgoConfig : Config) : java.util.Map[String, Object] = {
	   val configParams = new java.util.HashMap[String, Object]()
	   val partIdOrds = getOptionalIntListParam(appConfig, "partitionId.ordinals");
	   val idOrdinals = partIdOrds match {
	     case Some(idOrdinals: java.util.List[Integer]) => BasicUtils.fromListToIntArray(idOrdinals)
	     case None => null
	   }
	   configParams.put("partition.idOrdinals", idOrdinals)
	   
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   configParams.put("attr.ordinals", attrOrds)
	   
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   configParams.put("field.delim.in", fieldDelimIn)

	   
	   predictorStrategy match {
	     case `predStrategyZscore` => {
	       val attWeightList = getMandatoryDoubleListParam(appAlgoConfig, "attr.weights", "missing attribute weights")
	       val attrWeights = BasicUtils.fromListToDoubleArray(attWeightList)
	       configParams.put("attr.weights", attrWeights)
	       val statsFilePath = getMandatoryStringParam(appAlgoConfig, "stats.filePath", "missing stat file path")
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
	       
	     }
	     case `predStrategyEstAttrProb` => {
	       
	     }
	   }
	   
	   configParams
   }

}