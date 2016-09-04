import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class MatrixVectorMultiplyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line;
		String[] extracts;

		double bVal = 0.0;

		for(Text value : values) {
			line = value.toString();
			extracts = line.split(","); //Input: Matrix, i, value
			if(extracts[0].trim().equals("b")) {
				bVal = Double.parseDouble(extracts[2].trim());
			}
			else { //Output: i, value
				context.write(new IntWritable(Integer.parseInt(extracts[1])), new Text(extracts[0] + "," + (Double.parseDouble(extracts[2].trim()) * bVal)));
			}
		}
	}
}
