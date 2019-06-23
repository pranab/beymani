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
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val attrOrdsList = attrOrds.toList
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val outputOutliers = getBooleanParamOrElse(appConfig, "output.outliers", false)
	   val remOutliers = getBooleanParamOrElse(appConfig, "rem.outliers", false)
	   val cleanDataDirPath = getConditionalMandatoryStringParam(remOutliers, appConfig, "clean.dataDirPath", 
	       "missing clean data file output directory")
	   val seasonalTypeFldOrd = getOptionalIntParam(appConfig, "seasonal.typeFldOrd")
	   val windowSize = getIntParamOrElse(appConfig, "window.size", 3)
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val analyzerMap = creatSeasonalAnalyzerMap(this, appConfig, seasonalAnalysis)
	   
   }
}