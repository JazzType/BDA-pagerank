import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileOutputFormat;

public class MatrixVectorMultiplyMain {
	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: MatrixVectorMultiplyMain <input> <output_temp> <output>");
			System.exit(-1);
		}

		Job job1 = Job.getInstance();
		job1.setJarByClass(MatrixVectorMultiplyMain.class);
		job1.setJobName("MapReduce 1");

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.addInputPath(job1, new Path(args[1]));

		job1.setMapperClass(MatrixVectorMultiplyMapper.class);
		job1.setReducerClass(MatrixVectorMultiplyReducer.class);

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);

		if(job1.waitForCompletion(true) == 0) {
			Job job2 = Job.getInstance();
			job2.setJarByClass(MatrixVectorMultiplyMain.class);
			job2.setJobName("MapReduce 2");

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.addInputPath(job2, new Path(args[2]));

			job2.setMapperClass(MatrixVectorMultiplyAccumulateMapper.class);
			job2.setReducerClass(MatrixVectorMultiplyAccumulateReducer.class);

			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(IntWritable.class);			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
