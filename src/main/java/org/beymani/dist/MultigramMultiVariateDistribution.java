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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Multigram distribution on either single or multiple attributes
 * @author pranab
 *
 */
public class MultigramMultiVariateDistribution extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Muti variate distribution  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(MultigramMultiVariateDistribution.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(MultigramMultiVariateDistribution.HistogramMapper.class);
        job.setReducerClass(MultigramMultiVariateDistribution.HistogramReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("mmvd.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class HistogramMapper extends Mapper<LongWritable, Text, Tuple, IntWritable> {
		private String[] items;
		private Tuple outKey = new Tuple();
		private IntWritable outVal = new IntWritable(1);
        private String fieldDelimRegex;
        private RichAttributeSchema schema;
        private String keyCompSt;
        private Integer keyCompInt;
        private int numFields;
        private RichAttribute partitionField;
        private RichAttribute idField;
        private int[] fieldOrdinals;
        private Map<String, List<Object[]>> sequences = new HashMap<String, List<Object[]>>();
        private int fieldCount;
        private int sequenceLength;
        private String id;
        private String partitionAttr;
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
            
        	String filePath = conf.get("mmvd.histogram.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, RichAttributeSchema.class);
            
            numFields = schema.getFields().size();
            partitionField = schema.getPartitionField();
            idField = schema.getIdField();
            fieldOrdinals = Utility.intArrayFromString(conf.get("mmvd.hist.field.ordinals"));
            if (null != fieldOrdinals) {
            	fieldCount = fieldOrdinals.length;
            } else {
            	fieldCount = schema.getAttributeCount(true, true);
            }
            sequenceLength = conf.getInt("mmvd.sequence.length", 3);
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            if (items.length  != numFields){
            	context.getCounter("Data", "Invalid").increment(1);
            	return;
            }
            
            if(null != partitionField) {
            	partitionAttr = items[partitionField.getOrdinal()];
            }

            id = items[idField.getOrdinal()];
            List<Object[]> sequence = sequences.get(id);
            if (null == sequence) {
            	sequence = new ArrayList<Object[]>();
            	sequences.put(id, sequence);
            }
            
            Object[] rec;
            if (sequence.size() == sequenceLength) {
            	//full sequence, reuse record
            	rec = sequence.remove(0);
            } else {
            	//partial sequence
            	rec = new Object[fieldCount];
            }
            
            //build record
        	int j = 0;
            if (null != fieldOrdinals) {
            	//use specified fields only
            	for (int i : fieldOrdinals) {
            		RichAttribute field = schema.findAttributeByOrdinal(i);
	            	buildRec(rec, j, field);
	            	++j;
            	}
            } else {
            	//use all fields
	            for (RichAttribute field : schema.getFields()) {
	            	buildRec(rec, j, field);
	            	++j;
	            }
            }
            
            //add record to sequence
        	sequence.add(rec);
            if (sequence.size() == sequenceLength)  {
            	//emit
            	buildKey();
            	context.write(outKey, outVal);
            }
       }
        
        /**
         * @param field
         */
        private void buildKey() {
        	outKey.initialize();
        	if (null != partitionAttr) {
        		outKey.add(partitionAttr);
        	}
        	List<Object[]> sequence = sequences.get(id);
        	//all records
        	for (Object[] rec : sequence) {
        		//all fields
        		for (Object field : rec) {
        			outKey.add(field);
        		}
        	}
        }
        
        /**
         * @param field
         */
        private void buildRec(Object[] rec, int index, RichAttribute field) {
        	if (!field.isId() && !field.isPartitionAttribute()) {
	        	String	item = items[field.getOrdinal()];
	        	if (field.isCategorical()){
	        		keyCompSt = item;
	        		rec[index] = keyCompSt;
	        	} else if (field.isInteger()) {
	        		keyCompInt = Integer.parseInt(item) /  field.getBucketWidth();
	        		rec[index] = keyCompInt;
	        	} else if (field.isDouble()) {
	        		keyCompInt = ((int)Double.parseDouble(item)) /  field.getBucketWidth();
	        		rec[index] = keyCompInt;
	        	} 
        	}
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class HistogramReducer extends Reducer<Tuple, IntWritable, NullWritable, Text> {
    	private Text valueOut = new Text();
    	private String fieldDelim ;
        private int count;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelim = conf.get("field.delim", "[]");
        }    	
        
    	protected void reduce(Tuple key, Iterable<IntWritable> values, Context context)
        	throws IOException, InterruptedException {
   		    count = 0;
        	for (IntWritable value : values){
        		count += value.get();
        	}   
        	
        	key.setDelim(fieldDelim);
        	valueOut.set(key.toString() + fieldDelim + count);
			context.write(NullWritable.get(), valueOut);
    	}
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultigramMultiVariateDistribution(), args);
        System.exit(exitCode);
	}

}
