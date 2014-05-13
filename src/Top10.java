import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;

public class Top10 {

	public static class Top10Mapper
			extends
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NullWritable, Text> {

		public Top10Mapper() {
			// TODO Auto-generated constructor stub
		}

		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t", 34);
			String visitorName = split[0];
			int visitorCount = Integer.parseInt(split[1]);

			// Add this record to our map with the reputation as the key
			repToRecordMap.put(visitorCount, new Text(visitorCount + "\t"
					+ visitorName));
			// If we have more than ten records, remove the one with the lowest
			// rep
			// As this tree map is sorted in descending order, the user with
			// the lowest reputation is the last key.
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Output our ten records to the reducers with a null key
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static class Top10Reducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		// setup
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		private Text outputValue = new Text();
		private int ctr = 0;

		public void reduce(NullWritable key, Iterator<Text> values,
				Context context) throws IOException, InterruptedException {
			while (values.hasNext()) {
				String[] valueStr = values.next().toString().split("\t");
				String visitorName = valueStr[1];
				int visitorCount = Integer.parseInt(valueStr[0]);
				outputValue.set("" + visitorCount + "\t" + visitorName);
				repToRecordMap.put(visitorCount, outputValue);
				// If we have more than ten records, remove the one with the
				// lowest rep
				// As this tree map is sorted in descending order, the user with
				// the lowest reputation is the last key.
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			for (Text t : repToRecordMap.values()) {
				// Output our ten records to the file system with a null key
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Job conf = new Job(new Configuration());
		conf.setJarByClass(Top10.class);

		conf.setJobName("Top10"); // job name

		conf.setOutputKeyClass(NullWritable.class); // see output.collect in map
													// --> this and the
													// following line are reduce
													// input pair so reduce
													// should have
													// Text-IntWritable pair as
													// input
		conf.setOutputValueClass(Text.class); // see output.collect
		conf.setMapperClass(Top10Mapper.class); // specifies the mapper class
		conf.setCombinerClass(Top10Reducer.class); // specifies the combiner
		conf.setReducerClass(Top10Reducer.class); // specifies the reducer

		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); // specifies the
																// Input
																// directory
																// /home/<anyname_or_netid>/input
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); // specifies
																	// the
																	// Output
																	// directory
																	// /home/<anyname_or_netid>/output

		conf.waitForCompletion(true);
	}

	// TODO code application logic here
}
