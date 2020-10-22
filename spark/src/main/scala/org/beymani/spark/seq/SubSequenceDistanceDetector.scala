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

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.control.Breaks._
import org.apache.spark.SparkContext
import org.beymani.spark.common.OutlierUtility
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.chombo.math.MathUtils
import org.beymani.util.SeequenceScoreAggregator

/**
 * Model free anomaly detection based on the distance of a subsequence with other subsequences in 
 * the same file or another reference file
 * @param keyFields
 * @return
*/
object SubSequenceDistanceDetector extends JobConfiguration with GeneralUtility with OutlierUtility {
  
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "subSequenceDistanceDetector"
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
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")	
	   val windowSize = getIntParamOrElse(appConfig, "window.size", 3)
	   val refFilePath = getOptionalStringParam(appConfig, "distr.file.path")
     val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
		 val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   
	   //input
		 var  sameRefData = false
	   val data = sparkCntxt.textFile(inputPath)
	   val keyedData  = refFilePath match {
	     case Some(filePath) => {
	       //separate reference file
	       data.cache
	       val refData = sparkCntxt.textFile(filePath)
	       getKeyedValueWithPrefixSeq(data, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd, 0).
	         union(getKeyedValueWithPrefixSeq(refData, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd, 1))
	     }
	     case None => {
	       //use same file for reference
	       sameRefData = true
	       getKeyedValueWithPrefixSeq(data, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd, 0)
	     }
	   }

	   val taggedData = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val values = v._2.toArray
	     val valuesPair = if (sameRefData) {
	       val tRecs = values.sortBy(v => v.getLong(1))
	       (tRecs, tRecs)
	     } else {
	       val tRecs = values.filter(v => v.getInt(0) == 0).sortBy(v => v.getLong(1))
	       val rRecs = values.filter(v => v.getInt(0) == 1).sortBy(v => v.getLong(1))
	       (tRecs, rRecs)
	     }
	     val tRecs = valuesPair._1
	     val rRecs = valuesPair._2
	     val tValues = tRecs.map(r => {
	       val line = r.getString(2)
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	       items(attrOrd).toDouble
	     })
	     
	     //quant value arrays
	     val rValues =  if (sameRefData) {
	       tValues
	     } else {
	       rRecs.map(r => {
	         val line = r.getString(2)
	         val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	         items(attrOrd).toDouble
	       })
	     }
	     
	     //distance based score
	     val scoreAggr = new SeequenceScoreAggregator(windowSize)
	     for (i <- 0 to rValues.length - windowSize) {
	       val score = getMinDist(tValues, i, rValues, windowSize, sameRefData)
	       if (i == 0) {
	         for (j <- 0 to windowSize - 1) {
	           scoreAggr.add(score)
	         } 
	       } else {
	         scoreAggr.add(score)
	       }
	     }
	     
	     //recors with score and tag
	     val recs = rRecs.map(r => r.getString(2))
	     val scores =  scoreAggr.getScores().asScala
	     val taggedRecs = recs.zip(scores).map(r => {
	       val rec = r._1
	       val score = r._2
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
	 * @param data
	 * @param fieldDelimIn
	 * @param keyLen
	 * @param keyFieldOrdinals
	 * @param seqFieldOrd
	 * @param gen
	 * @return
	 */
	def getKeyedValueWithPrefixSeq(data: RDD[String], fieldDelimIn:String, keyLen:Int, 
	    keyFieldOrdinals: Option[Array[Int]], seqFieldOrd:Int, preFix :Int) : RDD[(Record, Record)] =  {
	   data.map(line => {
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val key = Record(keyLen)
	     
	     keyFieldOrdinals match {
	      case Some(fieldOrds : Array[Int]) => {
	    	  for (kf <- fieldOrds) {
	    		  key.addString(items(kf))
			    }
	      }
	      case None => key.add("all")
	    }


	     val value = Record(3)
	     val seq = items(seqFieldOrd).toLong
	     value.addLong(preFix)
	     value.addLong(seq)
	     value.addString(line)
	     (key, value)
	   })	 
	  
	}
	
  /**
	* @param fields
	* @param fieldOrdinals
	* @param rec
	* @param defKey
	*/
	def getMinDist(tValues:Array[Double], tOffset:Int, rValues:Array[Double], windowSize:Int, sameRefData:Boolean) : Double = {
	  var mintDist = Double.MaxValue
	  for (i <- 0 to rValues.length - windowSize){
	    val offsetDif = if ((i - tOffset) > 0) i - tOffset else tOffset - i
	    val selffMatch = sameRefData && offsetDif < windowSize
	    if (!selffMatch) {
	      var dist = 0.0
	      var broke = false
	      breakable {
  	      for (j <- 0 to windowSize - 1) {
  	        val diff = tValues(tOffset + j) - rValues(i + j)
  	        dist += diff * diff
  	        if (dist > mintDist)
  	          broke = true
  	          break
  	      }
	      }
	      if (!broke) {
	        mintDist = dist
	      }
	    }
	  }
	  math.sqrt(mintDist / windowSize)
	}
   
  
}