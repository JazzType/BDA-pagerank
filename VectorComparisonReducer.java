import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class VectorComparisonReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int difference = 0;
		context.getConfiguration().setBoolean("isDiffZero", true);
		for(IntWritable value : values) {			
			difference = value - difference;
			}
			if(difference != 0) {
				context.getConfiguration().setBoolean("isDiffZero", false);
			}
			else { //Output: i, value
				//context.write(new IntWritable(Integer.parseInt(extracts[1])), new Text(extracts[0] + "," + (Integer.parseInt(extracts[2].trim()) * bVal)));
				multipleOutputs.write("diff-" + context.getConfiguration().get("runID")
				                      , key
				                      , new IntWritable(difference));
			}
		}
	}
}
