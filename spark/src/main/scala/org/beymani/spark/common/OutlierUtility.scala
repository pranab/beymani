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
import org.apache.spark.rdd.RDD

/**
 * @author pranab
 *
 */
trait OutlierUtility {
  
	/**
	 * @param outputOutliers
	 * @param remOutliers
	 * @param cleanDataDirPath
	 * @param fieldDelimIn
	 * @param fieldDelimOut
	 * @param thresholdNorm
	 * @param taggedData
	 * @param data
	 * @return
	 */
	def processTaggedData(outputOutliers : Boolean, remOutliers: Boolean, cleanDataDirPath: String,
	    fieldDelimIn:String, fieldDelimOut:String, thresholdNorm: Option[Double], 
	    taggedData:RDD[String], data:RDD[String]) : RDD[String] = {
	 var tData = taggedData
	 if (outputOutliers || remOutliers) {
	   tData = taggedData.filter(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val marker = items(items.length - 1)
		   marker.equals("O")
	   })
	   if (remOutliers) {
	     //additional output for input with outliers subtracted
	     tData = tData.map(line => {
		   val items = line.split(fieldDelimIn, -1)
	       val ar = items.slice(0, items.length - 2)
	       ar.mkString(fieldDelimOut)
	     })
	     
	     //remove outliers records
	     val cleanData =  data.subtract(taggedData)
	     cleanData.saveAsTextFile(cleanDataDirPath) 
	   }
	 } else {
	   //all or only records above a threshold
	   tData =  thresholdNorm match {
	     case Some(threshold:Double) => {
	       taggedData.filter(line => {
	         val items = line.split(fieldDelimIn, -1)
	         val score = items(items.length - 2).toDouble
	         score > threshold
	       })
	     }
	     case None => taggedData
	   }
	 }
	 
	 tData
	}
}