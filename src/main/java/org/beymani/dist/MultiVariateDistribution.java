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

package org.beymani.dist;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.mr.HistogramField;
import org.chombo.mr.HistogramSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

public  class MultiVariateDistribution extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Muti variate distribution  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(MultiVariateDistribution.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(MultiVariateDistribution.HistogramMapper.class);
        job.setReducerClass(MultiVariateDistribution.HistogramReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Utility.setConfiguration(job.getConfiguration());
        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	public static class HistogramMapper extends Mapper<LongWritable, Text, Tuple , Text> {
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private HistogramSchema schema;
        private String keyCompSt;
        private Integer keyCompInt;
        private int numFields;
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
            
        	String filePath = conf.get("histogram.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, HistogramSchema.class);
            
            numFields = schema.getFields().size();
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            if ( items.length  != numFields){
            	context.getCounter("Data", "Invalid").increment(1);
            	return;
            }
            
            outKey.initialize();
            for (HistogramField field : schema.getFields()) {
            	String	item = items[field.getOrdinal()];
            	if (field.isCategorical()){
            		keyCompSt = item;
            		outKey.add(keyCompSt);
            	} else if (field.isInteger()) {
            		 keyCompInt = Integer.parseInt(item) /  field.getBucketWidth();
            		outKey.add(keyCompInt);
            	} else if (field.isDouble()) {
            		 keyCompInt = ((int)Double.parseDouble(item)) /  field.getBucketWidth();
            		outKey.add(keyCompInt);
            	} else if (field.isId()) {
            		outVal.set(item);
            	}
            }
        	context.getCounter("Data", "Processed record").increment(1);
			context.write(outKey, outVal);
       }
	}
	
    public static class HistogramReducer extends Reducer<Tuple, Text, NullWritable, Text> {
    	private Text valueOut = new Text();
    	private String fieldDelim ;
        private String itemDelim;
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelim = conf.get("field.delim", "[]");
        	itemDelim = conf.get("item.delim", ",");
        }    	
        
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
   		    StringBuilder stBld = new  StringBuilder();
   		    boolean first = true;
        	for (Text value : values){
        		if (first) {
        			stBld.append(value.toString());
        		} else {
        			stBld.append(itemDelim).append(value.toString());
        		}
        	}    	
        	key.setDelim(itemDelim);
        	valueOut.set(key.toString() + fieldDelim + stBld.toString());
			context.write(NullWritable.get(), valueOut);
    	}
    }
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultiVariateDistribution(), args);
        System.exit(exitCode);
	}
}
