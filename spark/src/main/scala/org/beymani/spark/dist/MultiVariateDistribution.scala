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
import org.chombo.spark.common.SeasonalUtility

object MultiVariateDistribution extends JobConfiguration with SeasonalUtility {
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
	   val shemaFilePath = this.getMandatoryStringParam(appConfig, "schema.filePath")
	   val schema = BasicUtils.getRichAttributeSchema(shemaFilePath)
       val numFields = schema.getFields().size()
       //val partitionField = schema.getPartitionField()
       val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }

       val idField = schema.getIdField()
       val idPresent = null != idField
	   val fieldOrdinals = getOptionalIntListParam(appConfig, "dist.fieldOrdinals") match {
	     case Some(ords : java.util.List[Integer]) => ords.asScala.toArray.map(v => v.toInt)
	     case None => schema.getAttributeOrdinals().map(v => v.toInt)
	   }
	   val fields = fieldOrdinals.map(i => schema.findAttribute(i)).filter(f => !f.isId() && !f.isPartitionAttribute())
	   val formatPrecision = this.getIntParamOrElse(appConfig, "format.precision", 6) 
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

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
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath).cache
	   
	   //count
	   val  recCount = keyFieldOrdinals match {
	     case Some(ordinals : Array[Integer]) => {
	       val count = data.map(line => {
	    	   val keyLen =  ordinals.length
	    	   val key = Record(keyLen)
	           val items = line.split(fieldDelimIn)
	           for (kf <- ordinals) {
	               key.addString(items(kf))
	           }
	    	   if (seasonalAnalysis) {
	    	     val i = ordinals.length
	    	     key.addString(items(i))
	    	     key.addString(items(i + 1))
	    	   }
	           (key.toString, line)
	       }).countByKey()
	       count
	     }
	     case None => {	    
	       val count = data.count()
	       Map("all" -> count)
	     }
	   }
	   
	   
	   //key by bucket
	   val bucketData = data.map(line => {
	     val items = line.split(fieldDelimIn)
	     //var len = fieldOrdinals.length + (if (null != partitionField)  1 else  0)
	     
	     var len = fieldOrdinals.length
	     len += (
	         keyFieldOrdinals match {
	         	case Some(ordinals : Array[Integer]) => ordinals.length
	         	case None => 0
	         }
	     )
	     if (seasonalAnalysis) {
	       len += 2
	     }
	     
	     val bucket = Record(len)
	     keyFieldOrdinals match {
	      case Some(ordinals : Array[Integer]) => {
	        val count = data.map(line => {
	           for (kf <- ordinals) {
	               bucket.addString(items(kf))
	           }
	        })
	    	if (seasonalAnalysis) {
	    	     val i = ordinals.length
	    	     bucket.addString(items(i))
	    	     bucket.addString(items(i + 1))
	    	 }
	      }
	      case None => 
	    }
	     
	     fields.foreach(f => {
	       f.getDataType() match {
	         case BaseAttribute.DATA_TYPE_STRING => {
	           bucket.addString(items(f.getOrdinal()))}
	         case BaseAttribute.DATA_TYPE_INT => {
	           val bin = items(f.getOrdinal()).toInt / f.getBucketWidth()
	           bucket.addInt(bin)}
	         case BaseAttribute.DATA_TYPE_FLOAT => {
	           val bin = (items(f.getOrdinal()).toFloat / f.getBucketWidth()).toInt
	           bucket.addInt(bin)}
	       }
	     })
	     
	     val value = Record(1)
	     if (null != idField) value.addString(items(idField.getOrdinal())) else value.addInt(1)
	     
	     (bucket, value)
	   })  
	   
	   //aggregate bins
	   val aggrData = bucketData.reduceByKey((v1, v2) => {
	     val nv = Record(1)
	     if (idPresent) {
	       nv.addString(v1.getString(0) + fieldDelimOut + v2.getString(0)) 
	     } else {
	       nv.addInt(v1.getInt(0) + v2.getInt(0))
	     }
	     nv
	   })
	   
	   //final result
	   val formAggrData = aggrData.map(kv => {
	     val res = if (idPresent) {
	       kv._2.getString(0)
	     } else {
	       val partId = keyFieldOrdinals match {
	       		case Some(ordinals : Array[Integer]) => {
	       		  var len = getKeyLen(ordinals, seasonalAnalysis)
	       		  kv._1.toString(0, len)
	       		}
	       		case None => {	    
	       			"all" 
	       		}
	       }	       
	       
	       val count = recCount.get(partId)
	       val dist =  count match {   
	         case Some(rCount:Long) =>   kv._2.getInt(0).toFloat / rCount 
	         case None => throw new IllegalStateException("missing count")
	       }
	       "" + kv._2.getInt(0) + fieldDelimOut + BasicUtils.formatDouble(dist, formatPrecision)
	     }	     
	     val bucket = keyFieldOrdinals match {
       		case Some(ordinals : Array[Integer]) => {
       		  var len = getKeyLen(ordinals, seasonalAnalysis)
       		  val key = kv._1.toString(0, len)
       		  val bucket = kv._1.toString(len, kv._1.size, ":")
       		  key + fieldDelimOut + bucket
       		}
       		case None => {	    
       			kv._1.toString(":")
       		}
	     }	       
	     
	     bucket + fieldDelimOut + res 
	   })
	   
       if (debugOn) {
         val records = formAggrData.collect
         records.foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     formAggrData.saveAsTextFile(outputPath) 
	   }
	   
   }
   
   /**
   * @param ordinals
   * @param seasonalAnalysis
   * @return
   */
   def getKeyLen(ordinals: Array[Integer], seasonalAnalysis:Boolean) : Int = {
	  var len = ordinals.length
   	  if (seasonalAnalysis) {
   		    len += 2
   	  }
      len
   }
}