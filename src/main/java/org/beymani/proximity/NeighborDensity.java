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

package org.beymani.proximity;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.SecondarySort;
import org.chombo.util.TextInt;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class NeighborDensity  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Nearest neighbour density";
        job.setJobName(jobName);
        
        job.setJarByClass(NeighborDensity.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(NeighborDensity.GroupingMapper.class);
        job.setReducerClass(NeighborDensity.GroupingReducer.class);
        
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(SecondarySort.TextIntIdPairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TextIntIdPairTuplePartitioner.class);

        Utility.setConfiguration(job.getConfiguration());

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class GroupingMapper extends Mapper<LongWritable, Text, TextInt, Tuple> {
        private String fieldDelimRegex;
        private String fieldDelim;
        private boolean isDensitySplit;
		private TextInt outKey = new TextInt();
		private Tuple outVal = new Tuple();
		private  String[] items ;
		private static final Logger LOG = Logger.getLogger(GroupingMapper.class);
				 
				 
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
           	fieldDelim = conf.get("field.delim", ",");
            fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
        	String densityFilePrefix = conf.get("ned.density.file.prefix", "first");
        	isDensitySplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(densityFilePrefix);
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             }
            LOG.debug("isDensitySplit:" + isDensitySplit);
        }    
	
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            outVal.initialize();
        	if (isDensitySplit) {
           		outKey.set(items[0], 0);
           	    outVal.add(Integer.parseInt(items[1]));
        	} else {
           		outKey.set(items[0], 1);
           	    outVal.add(items[1], Integer.parseInt(items[2]));
        	}
	   		context.write(outKey, outVal);
        }
        
	}
	
    /**
     * @author pranab
     *
     */
    public static class GroupingReducer extends Reducer<TextInt, Tuple, NullWritable, Text> {
    	private String entityID;
    	private int density;
    	private String group;
    	private Text outVal = new Text();
    	private String fieldDelim;
		private static final Logger LOG = Logger.getLogger(GroupingReducer.class);
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
           	fieldDelim = conf.get("field.delim", "\\[\\]");
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             }
        }
        
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(TextInt key, Iterable<Tuple> values, Context context)
            	throws IOException,  InterruptedException {
    		entityID = key.getFirst().toString();
			LOG.debug("entityID:" + entityID);
			
    		for (Tuple val : values) {
    			LOG.debug("value tuple size:" + val.getSize());
    			if (val.getSize() == 1) {
    				density = val.getInt(0);
    			} else {
    				group = val.getString(0);
    				outVal.set(group + fieldDelim + entityID + fieldDelim + density);
					context.write(NullWritable.get(), outVal);
    			}
    		}
    	}
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NeighborDensity(), args);
        System.exit(exitCode);
	}
    
    
}
