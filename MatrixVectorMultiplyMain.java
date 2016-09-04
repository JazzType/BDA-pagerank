import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.conf.Configuration;

enum DiffCounter {
	DIFF_COUNT
}

public class MatrixVectorMultiplyMain {
	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: MatrixVectorMultiplyMain <input-folder> <output_temp> <output>");
			System.exit(-1);
		}

		int runID = 0;
		long diffCount = 0;
		Configuration multiplyJobConfiguration = new Configuration();
		Configuration sumJobConfiguration = new Configuration();
		Configuration comparisonConfiguration = new Configuration();
		Job multiplyJob, sumJob, comparison;
		do {
			multiplyJobConfiguration.set("runID", "run" + runID);
			multiplyJob = Job.getInstance(multiplyJobConfiguration);
			multiplyJob.setJarByClass(MatrixVectorMultiplyMain.class);
			multiplyJob.setJobName("MapReduce " + runID + ": 1/2");
			
			MultipleInputs.addInputPath(multiplyJob, new Path(args[0] + "/matrix-a.txt"), TextInputFormat.class);
			if(runID == 0) {
				MultipleInputs.addInputPath(multiplyJob, new Path(args[0] + "/matrix-b.txt"), TextInputFormat.class);
			}
			else {
				MultipleInputs.addInputPath(multiplyJob, new Path(args[2] + "/run" + (runID - 1)), TextInputFormat.class);
			}
			FileOutputFormat.setOutputPath(multiplyJob, new Path(args[1] + "/run" + runID));
			multiplyJob.setMapperClass(MatrixVectorMultiplyMapper.class);
			multiplyJob.setReducerClass(MatrixVectorMultiplyReducer.class);

			multiplyJob.setOutputKeyClass(IntWritable.class);
			multiplyJob.setOutputValueClass(Text.class);
			
			multiplyJob.waitForCompletion(false);
			
			sumJobConfiguration.set("runID", "run" + runID);
			sumJob = Job.getInstance(sumJobConfiguration);	
			sumJob.setJarByClass(MatrixVectorMultiplyMain.class);
			sumJob.setJobName("MapReduce " + runID + ": 2/2");
			
			FileInputFormat.addInputPath(sumJob, new Path(args[1] + "/run" + runID));	
			FileOutputFormat.setOutputPath(sumJob, new Path(args[2] + "/run" + runID));
			sumJob.setMapperClass(MatrixVectorMultiplyAccumulateMapper.class);
			sumJob.setReducerClass(MatrixVectorMultiplyAccumulateReducer.class);

			sumJob.setOutputKeyClass(IntWritable.class);
			sumJob.setOutputValueClass(DoubleWritable.class);			
			
			sumJob.waitForCompletion(false);
			if(runID != 0) {
				comparisonConfiguration = new Configuration();
				comparison = Job.getInstance(comparisonConfiguration);	
				comparison.setJarByClass(MatrixVectorMultiplyMain.class);
				comparison.setJobName("MapReduce " + (runID) + ": 2/2");
			
				comparison.setMapperClass(VectorComparisonMapper.class);
				comparison.setReducerClass(VectorComparisonReducer.class);
	
				comparison.setOutputKeyClass(IntWritable.class);
				comparison.setOutputValueClass(DoubleWritable.class);
			
				MultipleInputs.addInputPath(comparison, new Path(args[2] + "/run" + (runID - 1) + "/part-r-00000"), TextInputFormat.class);			
				MultipleInputs.addInputPath(comparison, new Path(args[2] + "/run" + runID + "/part-r-00000"), TextInputFormat.class);
			FileOutputFormat.setOutputPath(comparison, new Path(args[2] + "/diff" + runID));
				comparison.waitForCompletion(false);
				diffCount = comparison.getCounters().findCounter(DiffCounter.DIFF_COUNT).getValue();
				System.out.println("Counter value: " + diffCount);
			}
			runID++;
		} while(diffCount != 4);
		System.out.println("Output to fin_out/run" + (runID - 1) + "/part-r-00000");
		System.exit(0);
	}		
}

