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

package org.beymani.spark.pc

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import org.chombo.util.BaseAttribute
import com.typesafe.config.Config
import org.beymani.spark.common.OutlierUtility
import org.chombo.spark.common.GeneralUtility
import org.avenir.util.PrincipalCompState
import org.chombo.math.MathUtils

/**
* PCA based outlier prediction
* @author pranab
* 
*/
object PrincipalComponentPredictor extends JobConfiguration with GeneralUtility {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "principalComponentPredictor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "id.field.ordinals"))
	   val quantFieldOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "quant.field.ordinals"))
	   val seqFieldOrd = getMandatoryIntParam( appConfig, "seq.field.ordinal", "missing sequence field ordinal") 
	   val dimension = quantFieldOrdinals.length
	   val stateFilePath = this.getMandatoryStringParam(appConfig, "state.filePath", "missing pc state file path")
	   val compState = PrincipalCompState.load(stateFilePath, fieldDelimOut).asScala.toMap
	   val scoreThreshold = getMandatoryDoubleParam(appConfig, "score.threshold", "missing score threshold")
	   val expConst = getDoubleParamOrElse(appConfig, "exp.const", 1.0)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //pc matrix and transposed pc matrix
	   val pcFun = (state: PrincipalCompState) => {
	     val pcArr = state.getPrincComps()
	     val pc = MathUtils.createMatrix(pcArr)
	     val pcTr =  pc.transpose()
	     (pc, pcTr)
	   }
	   val pcMa = updateMapValues(compState, pcFun)
	   
	   
	   val data = sparkCntxt.textFile(inputPath).cache
	   val taggedData = data.map(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val keyRec = Record(items, keyFieldOrdinals)
		   val keyStr = keyRec.toString(fieldDelimIn)
		   val quantFields = BasicUtils.extractFieldsAsDoubleArray(items, keyFieldOrdinals) 
		   var score = 0
		   val tag = pcMa.get(keyStr) match {
		     case Some(pc) => {
		       val pcHidden = pc._1
		       val pcNorm = pc._2
		       val daNorm = MathUtils.createColMatrix(quantFields)
		       
		       //regenerate
		       val daHideen = MathUtils.multiplyMatrix(pcHidden, daNorm)
		       val daRegen = MathUtils.multiplyMatrix(pcNorm, daHideen)
		       
		       //error
		       val quantFieldsGen = MathUtils.arrayFromColumnMatrix(daRegen)
		       var score = MathUtils.vectorDiffNorm(quantFields, quantFieldsGen)
		       if (expConst > 0) {
		    	   score = BasicUtils.expScale(expConst, score)
		       }		       
		       if (score < scoreThreshold) "N" else "O"
		     }
		     case None => "I"
		   }
		   val newRec = new Array[String](items.length + 2)
		   Array.copy(items, 0, newRec, 0, items.length)
		   newRec(newRec.length-2) = BasicUtils.formatDouble(score, precision)
		   newRec(newRec.length-1) = tag
		   (keyRec, newRec)
	   })
	   
	   //group by key and sort by sequence
	   val serTaggedData = groupByKeySortBySeq(taggedData, seqFieldOrd, fieldDelimOut)
	   
	   if (debugOn) {
         val records = serTaggedData.collect
         records.slice(0, 50).foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     serTaggedData.saveAsTextFile(outputPath) 
	   }	 
	   
   }
}