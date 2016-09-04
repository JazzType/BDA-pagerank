import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.Float;

public class MatrixVectorMultiplyAccumulateMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); //Input: row, value
		String[] extracts = line.split(",");
		context.write(new IntWritable(Integer.parseInt(extracts[0].split("\t")[0].trim())), new DoubleWritable(Double.parseDouble(extracts[1].trim())));
		}
	}

