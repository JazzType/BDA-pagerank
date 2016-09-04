import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;

public class VectorComparisonReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private MultipleOutputs<IntWritable, IntWritable> multipleOutputs;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<IntWritable, IntWritable>(context);
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int difference = 0;
		context.getConfiguration().setBoolean("isDiffZero", true);
		for(IntWritable value : values) {			
			difference = value.get() - difference;
			}
			if(difference != 0) {
				context.getConfiguration().setBoolean("isDiffZero", false);
				//break;
			}
			/*else { //Output: i, value
				//context.write(key, new IntWritable(difference));
				multipleOutputs.write(context.getConfiguration().get("runID")
				                      , key
				                      , new IntWritable(difference));
        
		}*/
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
	
}

