import java.io.IOException;
import java.lang.Float;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixVectorMultiplyReducer extends Reducer<IntWritable, Text, IntWritable, FloatWritable> {
	float bVal = 0;
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line;
		String[] extracts;
//		int bVal = -1;
		//StringBuilder reducerOutputString = new StringBuilder();
		for(Text value : values) {
			line = value.toString();
			extracts = line.split(","); //Input: Matrix, i, value
			if(extracts[0].trim().equals("b")) {
				bVal = Float.parseFloat(extracts[2].trim());
			}
			else { //Output: i, value
				context.write(new IntWritable(Integer.parseInt(extracts[1].trim())), new FloatWritable(Float.parseFloat(extracts[2].trim()) * bVal));
				
				//context.write(key, value);
			}
		}

	}
}
