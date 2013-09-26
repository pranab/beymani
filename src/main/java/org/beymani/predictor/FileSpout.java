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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author pranab
 *
 */
public class FileSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Map conf;
    private File[] files;
    private Scanner scanner;
    /**
     * 
     */
    private int curFileIndex = 0;
    
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
		
		String dirPath = conf.get("file.spout.dir.path").toString();
		File dir = new File(dirPath);
		files = dir.listFiles();
		Arrays.sort(files, new Comparator<File>(){
		    public int compare(File f1, File f2) {
		    	int res = f1.lastModified() < f2.lastModified() ? -1 : ( f1.lastModified() > f2.lastModified() ? 1 : 0);
		        return res;
		    } });
		
		openNextFile();
	}

	@Override
	public void nextTuple() {
		String record = readFile();
		String[] items = record.split("\\s+");
		String entityID = items[0];
		String recordData = items[1];
		collector.emit(new Values(entityID, recordData));
	}

	/**
	 * @return
	 */
	private String readFile() {
		String record = null;
		if (scanner.hasNextLine()) {
			 record =  scanner.nextLine();
		 } else {
			 if (++curFileIndex < files.length) {
					openNextFile();
					if (scanner.hasNextLine()) {
						 record =  scanner.nextLine();
					 }				 
			 } else {
				 //no more files to read
			 }
		 }
		return record;
	}
	
	/**
	 * 
	 */
	private void openNextFile() {
		try {
			scanner = new Scanner(files[curFileIndex]);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("file not found");
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entityID", "recordData"));		
	}
	
}	
