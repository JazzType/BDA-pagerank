import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixVectorMultiplyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line;
		String[] extracts;
		int bVal;
		//StringBuilder reducerOutputString = new StringBuilder();
		for(Text value : values) {
			line = value.toString();
			extracts = line.split(","); //Input: Matrix, i, value
			if(extracts[0].trim().equals("b")) {
				bVal = Integer.parseInt(extracts[2]);
			}
			else { //Output: i, value
				context.write(new IntWritable(Integer.parseInt(extracts[1])), new Text(extracts[0] + "," + (Integer.parseInt(extracts[2].trim()) * bVal)));
				//reducerOutputString.append("?" + );
			}
		}

	}
}
