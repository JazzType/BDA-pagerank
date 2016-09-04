import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;

public class VectorComparisonReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	
	private int diffCount = 1;
	private double difference = 0.0;
	@Override
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		for(DoubleWritable value : values) {			
			difference = value.get() - difference;
		}
		context.write(key, new DoubleWritable(difference));
		if(new Double(Math.abs(difference)).toString().startsWith("0.00") == true 
		   || new Double(Math.abs(difference)).toString().equals("0.0")) {
			context.write(key, new DoubleWritable(difference));
			System.out.println(diffCount++ + ": " + difference);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		if(diffCount == 4) {
			context.getCounter(DiffCounter.DIFF_COUNT).increment(diffCount);
		}
	}
	
}

