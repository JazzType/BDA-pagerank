import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixVectorMultiplyAccumulateReducer extends Reducer<IntWritable, FloatWritable, Text, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		float sum = 0;
		for(FloatWritable value : values) {
			sum += value.get();
		}
		context.write(new Text("b"), new Text(key + "\t" + key + "\t" + new FloatWritable(sum)));
	}
}
