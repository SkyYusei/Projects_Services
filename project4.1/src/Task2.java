import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text result = new Text();
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] values = line.split("\t");

			if (values.length != 2) {
				return;
			} else {
				String first = values[0];
				String second = values[1];
				result.set(first);
				word.set(second);
				context.write(result, word);
				String[] words = first.split("\\s+");
				List<String> passWords = new ArrayList<String>();
				for (String word : words) {
					if (!word.isEmpty()) {
						if (word.length() > 0) {
							String lower = word.toLowerCase();
							passWords.add(lower);
						}
					}
				}
				int size = passWords.size();
				if (size <= 1) {
					return;
				} else {
					StringBuilder sb1 = new StringBuilder();
					for (int i = 0; i < size - 1; i++) {
						String newWord = passWords.get(i);
						sb1.append(newWord);
						sb1.append(" ");
					}
					StringBuilder sb2 = new StringBuilder();
					sb2.append(passWords.get(size - 1));
					sb2.append(",");
					sb2.append(second);
					result.set(sb1.toString().trim());
					word.set(sb2.toString());
					context.write(result, word);
				}
			}
		}
	}

	public static class Reduce extends
			TableReducer<Text, Text, ImmutableBytesWritable> {
		private static Configuration conf;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			conf = context.getConfiguration();
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			PriorityQueue<Word> queue = new PriorityQueue<Word>();

			for (Text value : values) {
				String line = value.toString();
				String[] word = line.split(",");
				if (word.length == 2) {
					Word newWord = new Word(word[0], word[1]);
					queue.add(newWord);
				}
			}
			byte[] keyBytes = Bytes.toBytes(key.toString());
			Put put = new Put(keyBytes);
			int n = 0;
			boolean end = false;
			while (!queue.isEmpty() && n < conf.getInt("n", 5)) {
				Word tempWord = queue.remove();
				byte[] data = Bytes.toBytes("data");
				byte[] word = Bytes.toBytes(tempWord.word);
				byte[] phase = Bytes.toBytes(String.valueOf(tempWord.phase));
				put.add(data, word, phase);
				end = true;
				n++;
			}
			if (end) context.write(null, put);
		}
		
		private class Word implements Comparable<Word> {

			private String word;
			private int phase;

			public Word(String word, String phase) {
				this.word = word;
				this.phase = Integer.parseInt(phase);
			}

			@Override
			public int compareTo(Word other) {
				if (this.phase == other.phase)
					return this.word.compareTo(other.word);
				return other.phase - this.phase;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] arguments = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "Project4.1");
		job.setJarByClass(Task2.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		TableMapReduceUtil.initTableReducerJob("test", Reduce.class, job);

		List<String> o = new ArrayList<String>();
		for (String a : arguments) o.add(a);
		FileInputFormat.addInputPath(job, new Path(o.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(o.get(1)));

		job.waitForCompletion(true);
	}
}
