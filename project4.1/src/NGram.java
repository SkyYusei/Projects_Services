import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGram {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text ngram = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			boolean end;
			String line = value.toString();
			line = line.trim().replaceAll("[^a-zA-Z]", " ").toLowerCase();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			ArrayList<String> words = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				words.add(tokenizer.nextToken());
			}
			int size = words.size();
			for (int i = 0; i < size; i++) {
				StringBuilder sb = new StringBuilder();
				end = true;
				for (int j = 0; j < 5 && i + j < size; j++) {
					if (end) end = false;
					else sb.append(" ");
					String word = words.get(i + j);
					sb.append(word);
					ngram.set(sb.toString());
					context.write(ngram, one);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			if (sum > 2)
				context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "project4.1");
		job.setJarByClass(NGram.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);               
        
	}
}