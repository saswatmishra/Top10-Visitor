import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 
 * @author saswat
 */
public class Visitor {
	// Mapper implementation --------- processes one line at a time
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// map method
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line); // StringTokenizer
																	// splits
																	// the line
																	// into
																	// tokens
																	// separated
																	// by
																	// whitespaces.
			if (tokenizer.hasMoreTokens()) {
				word.set(line);
				output.collect(word, one); // emits a key-value pair of <
											// <word>, 1>
			}
		}
	}

	// Reducer implementation --------- sums up the values
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0; // reduce method
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Visitor.class); // Create a JobConf object
		conf.setJobName("wordcount104"); // job name

		conf.setOutputKeyClass(Text.class); // see output.collect in map -->
											// this and the following line are
											// reduce input pair so reduce
											// should have Text-IntWritable pair
											// as input
		conf.setOutputValueClass(IntWritable.class); // see output.collect

		conf.setMapperClass(Map.class); // specifies the mapper class
		conf.setCombinerClass(Reduce.class); // specifies the combiner
		conf.setReducerClass(Reduce.class); // specifies the reducer

		conf.setInputFormat(TextInputFormat.class); // input type is text, key
													// is line no., value is the
													// line words
		conf.setOutputFormat(TextOutputFormat.class); // output type is text

		FileInputFormat.setInputPaths(conf, new Path(args[0])); // specifies the
																// Input
																// directory
																// /home/<anyname_or_netid>/input
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); // specifies
																	// the
																	// Output
																	// directory
																	// /home/<anyname_or_netid>/output

		JobClient.runJob(conf); // calling the JobClient.runJob to submit the
								// job and monitor progress
	}
}
