import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.conf.Configuration;

public class MatrixVectorMultiplyAccumulateReducer extends Reducer<IntWritable, DoubleWritable, Text, Text> {

	private MultipleOutputs<Text, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		for(DoubleWritable value : values) {
			sum += value.get();
		}
		context.write(new Text("b," + key.get() + "," + key.get() + ","), new Text("" + sum));
		/*
		multipleOutputs.write(context.getConfiguration().get("runID")
													, new Text("b," + key.get() + "," + key.get() + ",")
													, new Text("" + sum));
		*/
	}
	

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
