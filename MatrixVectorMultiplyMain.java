import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class MatrixVectorMultiplyMain {
	public static void main(String[] args) throws Exception {
		if(args.length != 4) {
			System.err.println("Usage: MatrixVectorMultiplyMain <input1> <input2> <output_temp> <output>");
			System.exit(-1);
		}

		int runID = 0;
		Configuration job1Configuration = new Configuration();
		Configuration job2Configuration = new Configuration();
		job1Configuration.set("runID", "run-" + runID);
		Job job1 = Job.getInstance(job1Configuration);
		job1.setJarByClass(MatrixVectorMultiplyMain.class);
		job1.setJobName("MapReduce " + runID + ": 1/2");
		/*
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.addInputPath(job1, new Path(args[1]));
		*/
		MultipleInputs.addInputPath(job2, new Path(args[1] + "/matrix-a.txt"), TextInputFormat.class);
		MultipleInputs.addInputPath(job2, new Path(args[1] + "/matrix-b.txt"), TextInputFormat.class);

		job1.setMapperClass(MatrixVectorMultiplyMapper.class);
		job1.setReducerClass(MatrixVectorMultiplyReducer.class);

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);


		job1.waitForCompletion(false) == 0)
		job2Configuration.set("runID", "run-" + runID);
		Job job1 = Job.getInstance(job1Configuration);	
		job2.setJarByClass(MatrixVectorMultiplyMain.class);
		job2.setJobName("MapReduce " + runID-1 + ": 2/2");
		
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.addInputPath(job2, new Path(args[2]));
	
/*			MultipleInputs.addInputPath(job2, new Path(args[2] + "/run-" + runID-1 + "-r-00000"), TextInputFormat.class);
		MultipleInputs.addInputPath(job2, new Path(args[2] + "/run-" + runID + "-r-00000"), TextInputFormat.class);
*/
		job2.setMapperClass(MatrixVectorMultiplyAccumulateMapper.class);
		job2.setReducerClass(MatrixVectorMultiplyAccumulateReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);			
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
		job2.waitForCompletion(false);

		Configuration comparisonConf = new Configuration();
		comparisonConf.set("runID", "run-" + runID);
		Job comparison = Job.getInstance(comparisonConf);		
		comparison.waitForCompletion(false);
		System.out.println("Boolean value: " + comparisonConf.getBoolean("isDiffZero"));
		System.exit(0);
		}		
	}
}
