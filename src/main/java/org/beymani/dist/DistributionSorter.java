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
import java.util.List;

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
import org.chombo.util.RichAttribute;
import org.chombo.util.RichAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author pranab
 *
 */
public class DistributionSorter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Muti variate distribution sorter  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(DistributionSorter.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(DistributionSorter.SorterMapper.class);
        job.setReducerClass(DistributionSorter.SorterReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("dis.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class SorterMapper extends Mapper<LongWritable, Text, Tuple , Text> {
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private RichAttributeSchema schema;
        private String[] bucketKeys;
        private String  bucketValues;
        private String itemDelim;
        private Integer valueCount;
        private byte[] bucketKeyTypes;
        
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
            
        	String filePath = conf.get("dis.histogram.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, RichAttributeSchema.class);
        	itemDelim = conf.get("dis.item.delim", ",");
        	
        	List<Byte> dataTypes = new ArrayList<Byte>();
            for (RichAttribute field : schema.getFields()) {
            	if (field.isCategorical()){
            		dataTypes.add( Tuple.STRING);
            	} else if (field.isInteger() || field.isDouble()) {
            		dataTypes.add(Tuple.INT);
            	}
            }
            
            bucketKeyTypes = new byte[dataTypes.size()];
            for (int i = 0; i < bucketKeyTypes.length; ++i) {
            	bucketKeyTypes[i] = dataTypes.get(i);
            }
            System.out.println("Num bucket key types:" + bucketKeyTypes.length);
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            if ( items.length  != 2){
            	context.getCounter("Data", "Invalid").increment(1);
            	return;
            }
            bucketKeys = items[0].split(itemDelim);
            bucketValues = items[1];
            valueCount = bucketValues.split(itemDelim).length;
            
            outKey.initialize();
            outKey.add(bucketKeyTypes, bucketKeys);
            outKey.prepend(valueCount);
            outVal.set(bucketValues);
        	context.getCounter("Data", "Processed record").increment(1);
			context.write(outKey, outVal);
       }
	}

    /**
     * @author pranab
     *
     */
    public static class SorterReducer extends Reducer<Tuple, Text, NullWritable, Text> {
    	private int itemCount;
    	private int maxItemCount;
    	private String itemDelim;
    	boolean filtered;
    	
    	protected void setup(Context context) throws IOException, InterruptedException {
 			Configuration conf = context.getConfiguration();
 			maxItemCount = conf.getInt("dis.max.item.count", -1);
 			itemCount = 0;
        	itemDelim = conf.get("dis.item.delim", ",");
        	filtered = maxItemCount > 0;
         }    	
       
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		if (!filtered || itemCount < maxItemCount ) {
	        	for (Text value : values){
	    			context.write(NullWritable.get(), value);
	    			if (filtered) {
	    				itemCount += value.toString().split(itemDelim).length;
	    			}
	        	}   
    		}
    	}
    }
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DistributionSorter(), args);
        System.exit(exitCode);
	}

}
