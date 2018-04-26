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
import org.chombo.util.RichAttribute;
import org.chombo.util.RichAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Multivariate distribution
 * @author pranab
 *
 */
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
        int numReducer = job.getConfiguration().getInt("mvd.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	public static class HistogramMapper extends Mapper<LongWritable, Text, Tuple , Text> {
		private String[] items;
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private RichAttributeSchema schema;
        private String keyCompSt;
        private Integer keyCompInt;
        private int numFields;
        private RichAttribute partitionField;
        private RichAttribute idField;
        private int[] fieldOrdinals;
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
            
        	//schema
            schema = Utility.getRichAttributeSchema(conf, "mvd.histogram.schema.file.path");
            
            numFields = schema.getFields().size();
            partitionField = schema.getPartitionField();
            idField = schema.getIdField();
            fieldOrdinals = Utility.intArrayFromString(conf.get("mvd.hist.field.ordinals"));
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            if ( items.length  != numFields){
            	context.getCounter("Data", "Invalid").increment(1);
            	return;
            }
            
            outKey.initialize();
            if(null != partitionField) {
            	outKey.add(items[partitionField.getOrdinal()]);
            }
            
            if (null != fieldOrdinals) {
            	//use specified fields only
            	for (int i : fieldOrdinals) {
            		RichAttribute field = schema.findAttributeByOrdinal(i);
	            	buildKey(field);
            	}
        		String	item = items[idField.getOrdinal()];
        		outVal.set(item);
            } else {
            	//use all fields
	            for (RichAttribute field : schema.getFields()) {
	            	buildKey(field);
	            	if (field.isId()) {
	            		String	item = items[field.getOrdinal()];
	            		outVal.set(item);
	            	}
	            }
            }
        	context.getCounter("Data", "Processed record").increment(1);
			context.write(outKey, outVal);
       }
        
        /**
         * @param field
         */
        private void buildKey(RichAttribute field) {
        	if (!field.isId() && !field.isPartitionAttribute()) {
	        	String	item = items[field.getOrdinal()];
	        	if (field.isCategorical()){
	        		keyCompSt = item;
	        		outKey.add(keyCompSt);
	        	} else if (field.isInteger()) {
	        		keyCompInt = Integer.parseInt(item) /  field.getBucketWidth();
	        		outKey.add(keyCompInt);
	        	} else if (field.isFloat()) {
	        		keyCompInt = ((int)Float.parseFloat(item)) /  field.getBucketWidth();
	        		outKey.add(keyCompInt);
	        	} else if (field.isDouble()) {
	        		keyCompInt = ((int)Double.parseDouble(item)) /  field.getBucketWidth();
	        		outKey.add(keyCompInt);
	        	} 
        	}
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class HistogramReducer extends Reducer<Tuple, Text, NullWritable, Text> {
    	private Text valueOut = new Text();
    	private String fieldDelim ;
        private String itemDelim;
        private boolean outputCount;
        private int count;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelim = conf.get("field.delim", "[]");
        	itemDelim = conf.get("mvd.item.delim", ",");
        	outputCount = conf.getBoolean("mvd.output.count", false);
        }    	
        
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
   		    StringBuilder stBld = new  StringBuilder();
   		    boolean first = true;
   		    count = 0;
        	for (Text value : values){
        		if (outputCount) {
        			++count;
        		} else {
	        		if (first) {
	        			stBld.append(value.toString());
	        			first = false;
	        		} else {
	        			stBld.append(itemDelim).append(value.toString());
	        		}
        		}
        	}   
        	
        	key.setDelim(itemDelim);
        	if (outputCount) {
        		valueOut.set(key.toString() + fieldDelim + count);
        	} else {
        		valueOut.set(key.toString() + fieldDelim + stBld.toString());
        	}
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
