import java.io.IOException;
import java.lang.Float;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MatrixVectorMultiplyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	private MultipleOutputs<IntWritable, Text> multipleOutputs;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<IntWritable, Text>(context);
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line;
		String[] extracts;

		int bVal = -2;

		for(Text value : values) {
			line = value.toString();
			extracts = line.split(","); //Input: Matrix, i, value
			if(extracts[0].trim().equals("b")) {
				bVal = Integer.parseInt(extracts[2].trim());
			}
			else { //Output: i, value
				//context.write(new IntWritable(Integer.parseInt(extracts[1])), new Text(extracts[0] + "," + (Integer.parseInt(extracts[2].trim()) * bVal)));
				multipleOutputs.write(context.getConfiguration().get("runID")
				                      , new IntWritable(Integer.parseInt(extracts[1]))
				                      , new Text(extracts[0] + "," + (Integer.parseInt(extracts[2].trim()) * bVal)));
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
