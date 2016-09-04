import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.conf.Configuration;

public class MatrixVectorMultiplyMain {
	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: MatrixVectorMultiplyMain <input-folder> <output_temp> <output>");
			System.exit(-1);
		}

		int runID = 0;
		Configuration job1Configuration = new Configuration();
		Configuration job2Configuration = new Configuration();
		job1Configuration.set("runID", "run" + runID);
		Job job1 = Job.getInstance(job1Configuration);
		job1.setJarByClass(MatrixVectorMultiplyMain.class);
		job1.setJobName("MapReduce " + runID + ": 1/2");
		/*
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		*/
		//FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		MultipleInputs.addInputPath(job1, new Path(args[0] + "/matrix-a.txt"), TextInputFormat.class);
		MultipleInputs.addInputPath(job1, new Path(args[0] + "/matrix-b.txt"), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setMapperClass(MatrixVectorMultiplyMapper.class);
		job1.setReducerClass(MatrixVectorMultiplyReducer.class);

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
		MultipleOutputs.addNamedOutput(job1, "run0",TextOutputFormat.class, IntWritable.class, Text.class);
	
		job1.waitForCompletion(false);
		//runID++;
		job2Configuration.set("runID", "run" + runID);
		Job job2 = Job.getInstance(job2Configuration);	
		job2.setJarByClass(MatrixVectorMultiplyMain.class);
		job2.setJobName("MapReduce " + runID + ": 2/2");
		
		FileInputFormat.addInputPath(job2, new Path(args[1] + "/run" + runID + "-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	
/*			MultipleInputs.addInputPath(job2, new Path(args[2] + "/run-" + runID-1 + "-r-00000"), TextInputFormat.class);
		MultipleInputs.addInputPath(job2, new Path(args[2] + "/run-" + runID + "-r-00000"), TextInputFormat.class);
*/
		job2.setMapperClass(MatrixVectorMultiplyAccumulateMapper.class);
		job2.setReducerClass(MatrixVectorMultiplyAccumulateReducer.class);

		MultipleOutputs.addNamedOutput(job2, "run0",TextOutputFormat.class, Text.class, Text.class);

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);			
		
		job2.waitForCompletion(false);

		Configuration comparisonConf = new Configuration();
		comparisonConf.set("runID", "diff" + runID);
		comparisonConf.setBoolean("isDiffZero", false);
		Job comparison = Job.getInstance(comparisonConf);	
		comparison.setJarByClass(MatrixVectorMultiplyMain.class);
		comparison.setJobName("MapReduce " + (runID) + ": 2/2");
		
		comparison.setMapperClass(VectorComparisonMapper.class);
		comparison.setReducerClass(VectorComparisonReducer.class);

		comparison.setOutputKeyClass(IntWritable.class);
		comparison.setOutputValueClass(IntWritable.class);
		
		MultipleOutputs.addNamedOutput(comparison, "diff0",TextOutputFormat.class, IntWritable.class, IntWritable.class);
		
		FileInputFormat.addInputPath(comparison, new Path(args[2] + "/run" + runID + "-r-00000"));
		FileOutputFormat.setOutputPath(comparison, new Path(args[2] + "-diff"));
		comparison.waitForCompletion(false);
		System.out.println("Boolean value: " + comparisonConf.getBoolean("isDiffZero", true));
		System.exit(0);
	}		
}

