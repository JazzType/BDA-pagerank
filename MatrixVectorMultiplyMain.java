import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixVectorMultiplyMain {
	public static void main(String[] args) throws Exception {
		if(args.length != 4) {
			System.err.println("Usage: MatrixVectorMultiplyMain <input1> <input2> <output_temp> <output>");
			System.exit(-1);
		}
	
		Path matrix = new Path(args[0]);
		Path matrix2 = new Path(args[1]);
		Job job1 = Job.getInstance();
		job1.setJarByClass(MatrixVectorMultiplyMain.class);
		job1.setJobName("MapReduce 1");

		//FileInputFormat.addInputPath(job1, new Path(args[0]));
		MultipleInputs.addInputPath(job1, matrix, TextInputFormat.class, MatrixVectorMultiplyMapper.class);
		MultipleInputs.addInputPath(job1, matrix2, TextInputFormat.class, MatrixVectorMultiplyMapper.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		//job1.setMapperClass(MatrixVectorMultiplyMapper.class);
		job1.setReducerClass(MatrixVectorMultiplyReducer.class);

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);

		if(job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance();
			job2.setJarByClass(MatrixVectorMultiplyMain.class);
			job2.setJobName("MapReduce 2");

			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			job2.setMapperClass(MatrixVectorMultiplyAccumulateMapper.class);
			job2.setReducerClass(MatrixVectorMultiplyAccumulateReducer.class);

			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(FloatWritable.class);			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

		
	}
}
