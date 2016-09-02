import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.Float;

public class MatrixVectorMultiplyAccumulateMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); //Input: row, value
		String[] extracts = line.split("\t");
		context.write(new IntWritable(Integer.parseInt(extracts[0].trim())), new FloatWritable(Float.parseFloat(extracts[1].trim())));
		}
	}

