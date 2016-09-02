import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixVectorMultiplyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); //Matrix, i, j, value
		String[] extracts = line.split(",");
		context.write(new IntWritable(Integer.parseInt(extracts[2])), new Text(extracts[0] + "," + extracts[1] + "," + extracts[3]));
		}
	}
}
