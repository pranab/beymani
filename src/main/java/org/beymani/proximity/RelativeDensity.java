package org.beymani.proximity;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.chombo.util.SecondarySort;
import org.chombo.util.TextInt;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

public class RelativeDensity  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Relative  density";
        job.setJobName(jobName);
        
        job.setJarByClass(RelativeDensity.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(NeighborDensity.GroupingMapper.class);
        job.setReducerClass(NeighborDensity.GroupingReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(SecondarySort.TextIntIdPairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TextIntIdPairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	public static class DensityMapper extends Mapper<LongWritable, Text, Text, Tuple> {
        private String fieldDelimRegex;
        private String fieldDelim;
        private  String[] items ;
		private Text outKey = new Text();
		private Tuple outVal = new Tuple();
        
        protected void setup(Context context) throws IOException, InterruptedException {
           	fieldDelim = context.getConfiguration().get("field.delim", ",");
            fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
            
        }		
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            outKey.set(items[0]);
            outVal.add(items[1], Integer.parseInt(items[2]));
	   		context.write(outKey, outVal);
        }
        
        
	}

}
