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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
        
        job.setMapperClass(RelativeDensity.DensityMapper.class);
        job.setReducerClass(RelativeDensity.DensityReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
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
        	outVal.initialize();
            items  =  value.toString().split(fieldDelimRegex);
            outKey.set(items[0]);
            outVal.add(items[1], Integer.parseInt(items[2]));
	   		context.write(outKey, outVal);
        }
	}

    /**
     * @author pranab
     *
     */
    public static class DensityReducer extends Reducer<Text, Tuple, NullWritable, Text> {
       	private String fieldDelim;
       	private String groupID;
       	private String entityID;
       	private int sumDensity;
       	private int density;
       	private int relDensity;
    	private Text outVal = new Text();
    	private int relDensityScale;
    	private static final Logger LOG = Logger.getLogger(DensityReducer.class);
    	
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
           	fieldDelim = conf.get("field.delim", ",");
           relDensityScale = context.getConfiguration().getInt("red.reltive.density.scale", 1000);
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             }
        }
    	
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Text key, Iterable<Tuple> values, Context context)
            	throws IOException,  InterruptedException {
    		groupID = key.toString();
			sumDensity = 0;
			density = 0;
    		for (Tuple val : values) {
    			entityID = val.getString(0);
    			if (entityID.equals(groupID)) {
    				density =  val.getInt(1);
    				LOG.debug("entityID:" + entityID + " density:" + density);
    			}
    			sumDensity += val.getInt(1);
    		}    
    		
    		relDensity = (density * relDensityScale) / sumDensity;
			outVal.set(groupID + fieldDelim +relDensity);
			context.write(NullWritable.get(), outVal);
    	}   
    	
    }	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RelativeDensity(), args);
        System.exit(exitCode);
	}
    
    
}
