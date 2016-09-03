import java.io.IOException;
import java.lang.Float;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixVectorMultiplyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); //Matrix, i, j, value
		String[] extracts = line.split(",");
		context.write(new IntWritable(Integer.parseInt(extracts[2].trim())), new Text(extracts[0].trim() + "," + extracts[1].trim() + "," + extracts[3].trim()));
		}
	}

