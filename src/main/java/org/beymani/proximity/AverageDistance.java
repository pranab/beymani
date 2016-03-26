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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.TextInt;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class AverageDistance extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Nearest neighbour stat calculation  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(AverageDistance.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(AverageDistance.TopMatchesMapper.class);
        job.setReducerClass(AverageDistance.TopMatchesReducer.class);
        
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(IdRankGroupComprator.class);
        job.setPartitionerClass(IdRankPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class TopMatchesMapper extends Mapper<LongWritable, Text, TextInt, Text> {
		private String srcEntityId;
		private String trgEntityId;
		private int rank;
		private TextInt outKey = new TextInt();
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelim;

        protected void setup(Context context) throws IOException, InterruptedException {
           	fieldDelim = context.getConfiguration().get("field.delim", ",");
            fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        }    
		
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            srcEntityId = items[0];
            trgEntityId = items[1];
            rank = Integer.parseInt(items[items.length - 1]);
            outKey.set(srcEntityId, rank);
            outVal.set(trgEntityId + fieldDelim + items[items.length - 1]);
			context.write(outKey, outVal);
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class TopMatchesReducer extends Reducer<TextInt, Text, NullWritable, Text> {
    	private int topMatchCount;
		private String trgEntityId;
		private String srcEntityId;
		private int count;
		private int sum;
		private int dist;
		private Text outVal = new Text();
		private boolean doAverage;
		private boolean doDensity;
		private boolean doGrouping;
		private int densityScale;
		private int avg;
		private int density;
        private String fieldDelim;
        private String fieldDelimRegex;
        private String[] items;
        private int grMemeberIndex;
        
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
           	fieldDelim = config.get("field.delim", ",");
            fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	topMatchCount = config.getInt("avd.top.match.count", 10);
            doAverage = config.getBoolean("avd.match.average", true);
            doDensity = config.getBoolean("avd.top.match.density", false);
            doGrouping = config.getBoolean("avd.top.match.grouping", false);
            densityScale = config.getInt("avd.top.match.density.scale", 1000000);
                        
         }
    	
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(TextInt key, Iterable<Text> values, Context context)
        	throws IOException,  InterruptedException {
    		srcEntityId  = key.getFirst().toString();
    		count = 0;
    		sum = 0;
    		grMemeberIndex = 0;
    		if (doGrouping) {
    			//group leader
    			outVal.set(srcEntityId + fieldDelim + srcEntityId + fieldDelim + grMemeberIndex);	
    			context.write(NullWritable.get(), outVal);
    			++grMemeberIndex;
    		}
    		
        	for (Text value : values){
        		 items  = value.toString().split(fieldDelimRegex);
        		
        		if (doAverage || doDensity) {
        			//average distance or density
        			dist = Integer.parseInt(items[1]);
        			sum += dist;
        		} else {
        			//neighborhood group - entity, group, memebership number
        			trgEntityId = items[0];
           			outVal.set(trgEntityId + fieldDelim + srcEntityId + fieldDelim + grMemeberIndex);	
        			context.write(NullWritable.get(), outVal);
        			++grMemeberIndex;
            	}
        		
        		if (++count == topMatchCount){
        			break;
        		}
        	} 
        	 
    		if (doAverage || doDensity) {
    			avg = sum / count;
    		}
    		
        	if (doDensity) {
        		avg = avg == 0 ? 1 : avg;
        		density =  densityScale / avg;
        		outVal.set(srcEntityId +fieldDelim + density);
        	} else if (doAverage) {
        		outVal.set(srcEntityId + fieldDelim + avg);
        	}
        	
    		if (doAverage || doDensity) {
    			context.write(NullWritable.get(), outVal);
    		}
    	}
    	
    }
	
    /**
     * @author pranab
     *
     */
    public static class IdRankPartitioner extends Partitioner<TextInt, Text> {
	     @Override
	     public int getPartition(TextInt key, Text value, int numPartitions) {
	    	 //consider only base part of  key
		     Text id = key.getFirst();
		     return id.hashCode() % numPartitions;
	     }
   }
    
    /**
     * @author pranab
     *
     */
    public static class IdRankGroupComprator extends WritableComparator {
    	protected IdRankGroupComprator() {
    		super(TextInt.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		Text t1 = ((TextInt)w1).getFirst();
    		Text t2 = ((TextInt)w2).getFirst();
    		
    		int comp = t1.compareTo(t2);
    		return comp;
    	}
     }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AverageDistance(), args);
        System.exit(exitCode);
	}
}
