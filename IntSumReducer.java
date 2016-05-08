import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {
	private FloatWritable result = new FloatWritable();
	int sum = 0;
	int count = 0;
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
	// Iterable because we do not know the end of the array so we have to iterate till the end of the array
			throws IOException, InterruptedException {
		
		for (FloatWritable val : values) {
			if (val.get() != -9999 && val.get() != -999) {
				sum += val.get();
				count++;
			}

		}
		float output = (sum / count);
		result.set(output);
		context.write(key, result);
	}
}