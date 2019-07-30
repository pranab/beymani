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

package org.beymani.spark.common

import scala.Array.canBuildFrom
import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.hoidla.window.SequenceClusterFinder

/**
 * Reduces outlier flooding  with temporal clustering
 * @author pranab
 */
object OutlierCluster extends JobConfiguration  with GeneralUtility  {
  
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
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrd", "missing seq field ordinal")
	   val keyLen = getMandatoryIntParam(appConfig, "key.length", "missing key length")
	   val clusterStrategy = getStringParamOrElse(appConfig, "cluster.strategy", "averageInterval")
	   val avInterval = getConditionalMandatoryIntParam(clusterStrategy.equals("averageInterval") || clusterStrategy.equals("both"), 
	       appConfig, "cluster.avInterval", "missing average interval")
	   val maxInterval = getConditionalMandatoryIntParam(clusterStrategy.equals("maxInterval") || clusterStrategy.equals("both"), 
	       appConfig, "cluster.maxInterval", "missing average interval")
	   val minClusterMemeber = getIntParamOrElse(appConfig, "cluster.minSzie", 3)
	   val clProtoStrategy = getStringParamOrElse(appConfig, "cluster.protoStrategy", "center")
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig,"save.output", true)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   	   
	   val taggedData = data.map(line => {
		 val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		 val key = Record(items, 0, keyLen)
		 (key, items)
	   }).groupByKey.flatMap(r => {
	     val values = r._2.toArray.sortBy(v => {
	       v(seqFieldOrd).toLong
	     })
	     
	     //outlier time stamps
	     val fiValues = values.filter(r => r(r.length-1).equals("O"))
	     val timeStampScores = fiValues.map(v => 
	       (java.lang.Long.parseLong(v(seqFieldOrd)), java.lang.Double.parseDouble(v(v.length-2))) )
	     val sequences = new java.util.ArrayList[java.lang.Long]()
	     val scores = new java.util.ArrayList[java.lang.Double]()
	     for (t <- timeStampScores) {
	       sequences.add(t._1)
	       scores.add(t._2)
	     }
	     
	     //temporal cluster
	     val clusFinder = new SequenceClusterFinder(sequences, avInterval, maxInterval,  clusterStrategy)
	     clusFinder.findClusters()
	     val prototypeList = 
	       if (clProtoStrategy.equals("center")) clusFinder.getPrototypes(minClusterMemeber)
	       else clusFinder.getPrototypes(minClusterMemeber, clProtoStrategy, scores)
	     val prototypes =  BasicUtils.flatten(prototypeList).asScala.toSet
	     
	     //append another field for cluster based tag
	     values.map(v => {
	       val ts = java.lang.Long.parseLong(v(seqFieldOrd))
	       var tag = ""
	       val curTag = v(v.length-1)
	       if (curTag.equals("I")) {
	         tag = "I"
	       } else {
	    	 tag = if (prototypes.contains(ts)) "O" else "N"
	       }
	       v.mkString(fieldDelimOut) + fieldDelimOut + tag
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