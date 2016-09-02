import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VectorComparisonMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); 
		String[] extracts = line.split(",");
		context.write(new IntWritable(Integer.parseInt(extracts[1].trim())), new IntWritable(extracts[3].trim()));
		}
	}
}
