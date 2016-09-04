import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;

public class VectorComparisonReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	
	//private boolean isDiffZero = true;
	
	@Override
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double difference = 0.0;
		//context.getConfiguration().setBoolean("isDiffZero", false);
		for(DoubleWritable value : values) {			
			difference = value.get() - difference;
		}
		if(difference < 0)
			difference = -difference;
		if(new Double(difference).toString().startsWith("0.00")) {
			context.getConfiguration().set("isDiffZero", "true");
			context.write(key, new DoubleWritable(difference));
		}
	}
}

