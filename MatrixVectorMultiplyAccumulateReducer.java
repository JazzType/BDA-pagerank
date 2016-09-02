import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOuputs;
import org.apache.hadoop.conf.Configuration;

public class MatrixVectorMultiplyAccumulateReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

	private MultipleOutputs<IntWritable, IntWritable> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<IntWritable, IntWritable>(context);
	}

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value : values) {
			sum += value.get();
		}
		multipleOutputs.write("run-" + context.getConfiguration().get("runID"), new Text("b," + key.get() + "," + key.get() + ","), new Text("" + sum));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
