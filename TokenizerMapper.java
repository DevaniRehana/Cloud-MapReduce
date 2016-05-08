/* 

Name: Rehana Devani
ID: 1001100807
Hadoop Project
CSE 6331: Cloud Computing-002
*/

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

	private FloatWritable temperaetureFloatWritable = new FloatWritable();

	public void map(Object key, Text value, Context context)
	// key input value input key output value output
			throws IOException, InterruptedException {
		String data = value.toString();
		String[] tokens = data.split("\n");
		
		for (int i = 0; i < tokens.length; i++) {
			String year = tokens[i].substring(0, 4);
			Text date_key = new Text(year);
			String[] split = tokens[i].split(" ");
			String temperature = null;
			String split6 = split[6];
			if(!split6.matches(".*\\d.*") && split[7].matches(".*\\d.*"))
			 temperature= split[7].replace(" ", "");
			else if(split6.matches(".*\\d.*") && !split[7].matches(".*\\d.*"))
				temperature= split[6].replace(" ", "");
			else
				temperature=split[8].replace(" ", "");
			String temperature1 = temperature + "repeat";
			
			
			if (!temperature1.equals("repeat")
					&& !temperature1.equals("-repeat")) {
				int temperature_value = Integer.parseInt(temperature);
				if (temperature_value != -9999 && temperature_value != -999) {
					temperaetureFloatWritable.set(temperature_value);
					context.write(date_key, temperaetureFloatWritable);
				}
			}
		}

	}
}
