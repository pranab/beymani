/*
 * beymani: Outlier and anamoly detection 
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


package org.beymani.predictor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.chombo.stats.HistogramStat;
import org.chombo.stats.HistogramUtility;
import org.chombo.storm.Cache;
import org.chombo.storm.MessageQueue;
import org.chombo.util.BasicUtils;
import org.chombo.util.ConfigUtility;
import org.chombo.util.RichAttribute;
import org.chombo.util.RichAttributeSchema;
import org.chombo.util.Utility;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author pranab
 *
 */
public abstract class DistributionBasedPredictor extends ModelBasedPredictor {
	protected MessageQueue outQueue;
	protected Cache cache;
	protected Map<String, Integer> distrModel = new HashMap<String, Integer>();
	protected Map<String, Map<String, Integer>> keyedDistrModel = new HashMap<String, Map<String, Integer>>();
	protected Map<String, HistogramStat> keyedHist = new HashMap<String, HistogramStat>();
	protected int totalCount;
	protected Map<String, Integer> totalCounts = new HashMap<String, Integer>();
	protected RichAttributeSchema schema;
	protected StringBuilder stBld = new StringBuilder();
	protected String subFieldDelim = ";";
	protected boolean seasonal;

	/**
	 * Storm usage
	 * @param conf
	 */
	public DistributionBasedPredictor(Map conf) {
		super();
		outQueue = MessageQueue.createMessageQueue(conf, conf.get("output.queue").toString());
		cache = Cache.createCache(conf);
		
		String modelKey =  conf.get("distribution.model.key").toString();
		String model = cache.get(modelKey);
		
		//distribution
		Scanner scanner = new Scanner(model);
		totalCount = 0;
		while (scanner.hasNextLine()) {
		  String line = scanner.nextLine();
		  String[] items = line.split(",");
		  int count = Integer.parseInt(items[1]);
		  totalCount += count;
		  distrModel.put(items[0], count);
		}
		
		//schema
		String schemaKey =  conf.get("schema.key").toString();
		String schemaStr = cache.get(schemaKey);
		ObjectMapper mapper = new ObjectMapper();
		try {
			schema = mapper.readValue(schemaStr, RichAttributeSchema.class);
		} catch (JsonParseException e) {
			throw new IllegalStateException("invalid JSON schema");
		} catch (JsonMappingException e) {
			throw new IllegalStateException("invalid JSON schema");
		} catch (IOException e) {
			throw new IllegalStateException("failed to parse JSON schema");
		}
		
		scoreThreshold =  Double.parseDouble(conf.get("score.threshold").toString());
	}
	
	/**
	 * Hadoop MR usage
	 * @param config
	 * @throws IOException 
	 */
	public DistributionBasedPredictor(Configuration config, String distrFilePath) throws IOException {
		super();

    	InputStream fs = Utility.getFileStream(config, distrFilePath);
    	BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    	String line = null; 
    	String[] items = null;
		
    	while((line = reader.readLine()) != null) {
    		items = line.split(",");
  		  	int count = Integer.parseInt(items[1]);
  		  	totalCount += count;
  		  	distrModel.put(items[0], count);
    	} 	
    	
    	schema = Utility.getRichAttributeSchema(config, "dbp.distr.schema.file.path");
    	scoreThreshold =  Double.parseDouble(config.get("dbp.score.threshold"));
	}
	
	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param distrFilePathParam
	 * @param hdfsFileParam
	 * @param schemaFilePathParam
	 * @param seasonalParam
	 * @param fieldDelimParam
	 * @param scoreThresholdParam
	 * @param pseudoMultiVariate
	 * @throws IOException
	 */
	/*
	public DistributionBasedPredictor(Map<String, Object> config, String idOrdinalsParam, 
			String distrFilePathParam, String hdfsFileParam,
			String schemaFilePathParam, String seasonalParam, String fieldDelimParam, 
			String scoreThresholdParam, boolean pseudoMultiVariate) throws IOException {
		super();
		if (pseudoMultiVariate) {
			initializePseudoMultiVariate(config, idOrdinalsParam, distrFilePathParam, hdfsFileParam,
					schemaFilePathParam, seasonalParam, fieldDelimParam, scoreThresholdParam);
		} else {
			initializeMultiVariate(config, idOrdinalsParam, distrFilePathParam, hdfsFileParam,
					schemaFilePathParam, seasonalParam, fieldDelimParam, scoreThresholdParam);
		}
		
	}
	 */
	
	/**
	 * @param config
	 * @param idOrdinalsParam
	 * @param distrFilePathParam
	 * @param hdfsFileParam
	 * @param schemaFilePathParam
	 * @param seasonalParam
	 * @param fieldDelimParam
	 * @param scoreThresholdParam
	 * @throws IOException
	 */
	public DistributionBasedPredictor(Map<String, Object> config, String idOrdinalsParam, 
			String attrListParam, String distrFilePathParam, String hdfsFileParam,
			String schemaFilePathParam, String seasonalParam, String fieldDelimParam, 
			String scoreThresholdParam) throws IOException {
		boolean hdfsFilePath = ConfigUtility.getBoolean(config, hdfsFileParam);
		String filePath = ConfigUtility.getString(config, distrFilePathParam);
		InputStream fs = null;
		if (hdfsFilePath) {
			fs = Utility.getFileStream(filePath);
		} else {
			fs = BasicUtils.getFileStream(filePath);
		}
		idOrdinals = ConfigUtility.getIntArray(config, idOrdinalsParam);
		attrOrdinals = ConfigUtility.getIntArray(config, attrListParam);
		seasonal = ConfigUtility.getBoolean(config, seasonalParam);
		int keyLen = idOrdinals.length;
		keyLen += (seasonal ? 2 : 0);
		++keyLen;
		String delim = ConfigUtility.getString(config, fieldDelimParam, ",");

		Map<String[], HistogramStat> keyedHist = HistogramUtility.createHiostograms(fs,  keyLen, false);
		for (String[] key : keyedHist.keySet()) {
			String compKey = BasicUtils.join(key, delim);
			this.keyedHist.put(compKey, keyedHist.get(key));
		}
		
       	String schemFilePath = ConfigUtility.getString(config, schemaFilePathParam);
    	schema = BasicUtils.getRichAttributeSchema(schemFilePath);
    	scoreThreshold = ConfigUtility.getDouble(config, scoreThresholdParam);
	}

	/**
	 * for multi variate distribution
	 * @param config
	 * @param distrFilePath
	 * @throws IOException
	 */
	public DistributionBasedPredictor(Map<String, Object> config, String idOrdinalsParam, String distrFilePathParam, 
			String hdfsFileParam, String schemaFilePathParam, String seasonalParam, String fieldDelimParam, 
			String scoreThresholdParam) throws IOException {
		idOrdinals = ConfigUtility.getIntArray(config, idOrdinalsParam);
		boolean hdfsFilePath = ConfigUtility.getBoolean(config, hdfsFileParam);
		String filePath = ConfigUtility.getString(config, distrFilePathParam);
		InputStream fs = null;
		if (hdfsFilePath) {
			fs = Utility.getFileStream(filePath);
		} else {
			fs = BasicUtils.getFileStream(filePath);
		}
		seasonal = ConfigUtility.getBoolean(config, seasonalParam);
		String delim = ConfigUtility.getString(config, fieldDelimParam, ",");
		
    	BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    	String line = null; 
    	String[] items = null;
    	String compKey = null;
    	
    	while((line = reader.readLine()) != null) {
    		items = line.split(",");
    		int i = 0;
    		if (null != idOrdinals) {
    			compKey = BasicUtils.join(items, 0, idOrdinals.length);
    			i += idOrdinals.length;
    		}
    		if (seasonal) {
    			compKey = compKey + delim + items[i] + delim + items[i+1];
    			i += 2;
    		}

    		String bucket = items[i++];
  		  	int count = Integer.parseInt(items[i++]);
  		  	
    		if (null != idOrdinals) {
    			Map<String, Integer> distrModel = keyedDistrModel.get(compKey);
    			if (null == distrModel) {
    				distrModel = new HashMap<String, Integer>();
    				keyedDistrModel.put(compKey, distrModel);
    				totalCounts.put(compKey, 0);
    			}
    			distrModel.put(bucket, count);
    			totalCounts.put(compKey, totalCounts.get(compKey) + count);
    		} else {  		  	
  		  		totalCount += count;
  		  		distrModel.put(bucket, count);
    		}
    	} 	
    	
    	String schemFilePath = ConfigUtility.getString(config, schemaFilePathParam);
    	schema = BasicUtils.getRichAttributeSchema(schemFilePath);
    	scoreThreshold = ConfigUtility.getDouble(config, scoreThresholdParam);
	}
	
	/**
	 * @param record
	 * @return
	 */
	protected String getBucketKey(String record) {
		String[] items = record.split(",");
		return getBucketKey(items);
	}
	
	/**
	 * @param items
	 * @return
	 */
	protected String getBucketKey(String[] items) {
		int i = 0;
		if (null != idOrdinals) {
			i += idOrdinals.length;
		}
		if (seasonal) {
			i += 2;
		}
		String bucket = items[i];
		return bucket;
	}
	
}
