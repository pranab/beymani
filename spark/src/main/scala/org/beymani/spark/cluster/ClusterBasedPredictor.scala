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

package org.beymani.spark.cluster

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import org.chombo.util.BaseAttribute
import com.typesafe.config.Config
import org.beymani.spark.common.OutlierUtility
import org.chombo.spark.common.GeneralUtility
import org.chombo.distance.InterRecordDistance
import org.avenir.cluster.ClusterData
import org.avenir.cluster.Cluster
import org.avenir.cluster.ClusterUtility
import org.chombo.spark.common.SeasonalUtility

/**
 * Outlier prediction based Cluster Based Local Outlier Factor (CBLOF)
 * @author pranab
 *
 */
object ClusterBasedPredictor extends JobConfiguration with GeneralUtility with SeasonalUtility {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "clusterBasedPredictor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "id.field.ordinals"))
	   val attrOrdinals = toIntArray(getMandatoryIntListParam(config, "attr.ordinals", "missing attribute field"))
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.field.ordinal","missing sequence field ordinal") 
	   val schemaPath = getMandatoryStringParam(appConfig, "schema.path", "missing schema file path")
	   val distSchemaPath = getMandatoryStringParam(appConfig, "dist.schemaPath", "missing distance schema file path")
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val largeClusterSizeFraction = getDoubleParamOrElse(appConfig, "cluster.largeSizeFraction", 0.9)
	   val largeClusterSizeMultilier = getDoubleParamOrElse(appConfig, "cluster.largeSizemultiplier", 5.0)
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")
	   val expConst = getDoubleParamOrElse(appConfig, "exp.const", 1.0)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val schema = BasicUtils.getGenericAttributeSchema(schemaPath)
	   val distSchema = BasicUtils.getDistanceSchema(distSchemaPath)
       val distanceFinder = new InterRecordDistance(schema, distSchema, fieldDelimIn)
       distanceFinder.withFacetedFields(attrOrdinals);
	   val keyLen = keyFieldOrdinals.length + (if (seasonalAnalysis) 2 else 0)
	   val clusterDataFilePath = getMandatoryStringParam(appConfig, "cluster.dataPath", "missing cluster data file path")
	   
	   val keyedClusters = ClusterUtility.load(clusterDataFilePath, fieldDelimOut, keyLen).asScala.toMap
	   val tarnsformer = (v:java.util.List[ClusterData]) => {
	     ClusterUtility.labelSize(v, largeClusterSizeFraction, largeClusterSizeMultilier)
	   }
	   updateMapValues(keyedClusters, tarnsformer)
	   val seasonalAnalyzers = creatOptionalSeasonalAnalyzerArray(this, appConfig, seasonalAnalysis)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   val taggedData = data.map(line => {
		 val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		 val keyRec = Record(keyLen, fields, keyFieldOrdinals)
		 addSeasonalKeys(seasonalAnalyzers, fields, keyRec)
		 val keyStr = keyRec.toString
		 val clusters = getMapValue(keyedClusters, keyStr, "missing cluster data for " + keyStr).asScala.toList
		 
		 //clusters sorted by distance
		 val clDist = clusters.map(cl => {
		   val dist = cl.findDistaneToCentroid(fields,  distanceFinder)
		   (cl, dist)
		 }).sortBy(v => v._2)
		 
		 val closestCl = clDist.head
		 var score = if (closestCl._1.isLarge()) {
		   //belongs to large cluster
		   closestCl._2
		 } else {
		   //belongs to small cluster, find distance to nearest large cluster
		   val clFound = clDist.find(r => r._1.isLarge())
		   val dist = clFound match {
		     case Some(clDi) => clDi._2
		     case None  => -1.0
		   }
		   BasicUtils.assertCondition(dist > 0, "data belong to small cluster, but no large cluster found")
		   dist
		 }
		 if (expConst > 0) {
		   score = BasicUtils.expScale(expConst, score)
		 }		       
		 val tag = if (score < scoreThreshold) "N" else "O"
		 line + fieldDelimOut + BasicUtils.formatDouble(score, precision) + fieldDelimOut + tag
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