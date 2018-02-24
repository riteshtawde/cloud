/*
Project 1
Members : Jimit Shah(jimshah@iu.edu), Ritesh Tawde(rtawde@iu.edu)
*/

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopMinMax {

	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		int mcount = 0;
		private static DoubleWritable min = new DoubleWritable(Double.MAX_VALUE); 
		private static DoubleWritable max = new DoubleWritable(Double.MIN_VALUE);		
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			/*testing*/
			//System.out.println("Mapper :: Executed "+(++mcount)+" times!!!");
			if (Double.parseDouble(value.toString()) < Map.min.get()) {
				min.set(Double.parseDouble(value.toString()));
				word.set("Min");
				context.write(word, min);
			}
			if (Double.parseDouble(value.toString()) > Map.max.get()) {
				max.set(Double.parseDouble(value.toString()));
				word.set("Max");
				context.write(word, max);
			}
			//System.out.println("Max till now : "+max.get()+" and min till now : "+min.get());
			
			word.set("Sum");
			context.write(word, new DoubleWritable(Double.parseDouble(value.toString())));
		}
	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		int rcount = 0;
		private DoubleWritable result = new DoubleWritable();
		private Text key = new Text();
		private ArrayList<Double> listOfValues = new ArrayList<Double>();
		
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			/*System.out.println("Reducer :: Executed "+(++rcount)+" times!!!");*/
			//System.out.println("Key : "+key.toString());
			if (key.toString().equals("Min")) {
				double min = Integer.MAX_VALUE;
				for (DoubleWritable val : values) {
					//System.out.println("Reducer:: min : val : "+val);
					if (min > val.get()) {
						min = val.get();
					}
				}
				result.set(min);
				context.write(key, result);
			} else if (key.toString().equals("Max")) {
				double max = Integer.MIN_VALUE;
				for (DoubleWritable val : values) {
					//System.out.println("Reducer:: max : val : "+val);
					if (max < val.get()) {
						max = val.get();
					}
				}
				result.set(max);
				context.write(key, result);
			} else {
				double count = 0;
				double sd = 0;
				double avg = 0;
				for (DoubleWritable val : values) {
					// System.out.println("Reducer:: else : val : "+val);
					listOfValues.add(val.get());
					count++;
					avg += val.get();
				}
				avg = avg / count;
				result.set(avg);
				key.set("Avg");
				context.write(key, result);
				for (Double val : listOfValues) {					
					sd += Math.pow((val - avg), 2);					
				}
				//System.out.println("Std Dev = "+sd);
				sd = Math.sqrt(sd / count);
				key.set("StdDev");
				result.set(sd);
				context.write(key, result);
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: HadoopMinMax <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "minmax");
		job.setJarByClass(HadoopMinMax.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// Add a combiner here, not required to successfully run the wordcount
		// program

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(DoubleWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
