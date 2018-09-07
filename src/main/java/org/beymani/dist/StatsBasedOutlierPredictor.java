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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.beymani.predictor.EsimatedAttrtibuteProbabilityBasedPredictor;
import org.beymani.predictor.EstimatedProbabilityBasedPredictor;
import org.beymani.predictor.ModelBasedPredictor;
import org.beymani.predictor.RobustZscorePredictor;
import org.beymani.predictor.ZscorePredictor;
import org.chombo.util.Utility;

/**
 * Various stats and model based outlier predictor
 * @author pranab
 *
 */
public class StatsBasedOutlierPredictor  extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for various stats based outlier prediction";
        job.setJobName(jobName);
        
        job.setJarByClass(StatsBasedOutlierPredictor.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "beymani");
        job.setMapperClass(StatsBasedOutlierPredictor.PredictorMapper.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class PredictorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text  outVal = new Text();
        private String predictorStartegy;
        private ModelBasedPredictor predictor;
		private String fieldDelim;
        private static final String PRED_STRATEGY_ZSCORE = "zscore";
        private static final String PRED_STRATEGY_ROBUST_ZSCORE = "robustZscore";
        private static final String PRED_STRATEGY_EST_PROB = "estimatedProbablity";
        private static final String PRED_STRATEGY_EST_ATTR_PROB = "estimatedAttributeProbablity";
             
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	predictorStartegy = config.get("sbop.predictor.startegy", PRED_STRATEGY_ZSCORE);
        	
        	if (predictorStartegy.equals(PRED_STRATEGY_ZSCORE)) {
        		predictor = new ZscorePredictor(config,  "sbop.id.field.ordinals",  "sbop.attr.list", "sbop.stats.file.path",  
        			"field.delim.regex", "sbop.attr.weight", "sbop.score.threshold");
        	} else if (predictorStartegy.equals(PRED_STRATEGY_ROBUST_ZSCORE)) {
        		predictor = new RobustZscorePredictor(config,  "sbop.id.field.ordinals",  "sbop.attr.list", 
        			"sbop.med.stats.file.path", "sbop.mad.stats.file.path", "field.delim.regex", 
        			"sbop.attr.weight",  "sbop.score.threshold", false);
        	} else if (predictorStartegy.equals(PRED_STRATEGY_EST_PROB)) {
        		predictor = new EstimatedProbabilityBasedPredictor(config,  "sbop.distr.file.path",   "sbop.score.threshold" );
        	} else if (predictorStartegy.equals(PRED_STRATEGY_EST_ATTR_PROB)) {
        		predictor = new EsimatedAttrtibuteProbabilityBasedPredictor(config,  "sbop.distr.file.path",  "sbop.attr.weight",  
        			"sbop.score.threshold", "field.delim.regex");
        	} else {
        		throw new IllegalArgumentException("ivalid predictor strategy");
        	}
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            double score = predictor.execute("",  value.toString());
            
            if (predictor.isScoreAboveThreshold()) {
            	outVal.set(value.toString() + fieldDelim + score);
            	context.write(NullWritable.get(), outVal);
            }
        }
	}	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StatsBasedOutlierPredictor(), args);
        System.exit(exitCode);
	}
	
}
