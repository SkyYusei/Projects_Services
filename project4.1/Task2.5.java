import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * MapReduce class for task2, generate language model
 * @author wudongdong
 *
 */
public class Task2 {
	
	/**
	 * The Map class
	 * @author wudongdong
	 *
	 */
	public static class ngramstMap extends Mapper<LongWritable, Text, Text, Text>
    {
        private Text phrase = new Text();
        private Text word = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] values = line.split("\t");
            
            // ignore invalid data
            if(values.length == 2){
            	phrase.set(values[0]);
                word.set(values[1]);
                context.write(phrase, word);
                
                // split with white space
                String[] words = values[0].split("\\s+");
                List<String> nonEmptyWords = new ArrayList<String>();
                // ignore white space
                for(String temp : words){
                	if(!temp.isEmpty() && temp.length() > 0){
                		nonEmptyWords.add(temp.toLowerCase());
                	}
                }
                
                String k = null;
                String v = null;
                int size = nonEmptyWords.size();
                // size larger than 1, get the word
                if(size > 1){
                	
                	// parse to generate phrase
                	StringBuilder sb = new StringBuilder();
                	for(int i = 0; i < size-1; i++){
                    	sb.append(nonEmptyWords.get(i));
                		sb.append(" ");
                    }
                	k = sb.toString();
            		k = k.trim();
                	
                	// generate value as 'word,count'
                    sb = new StringBuilder();
                    sb.append(nonEmptyWords.get(size - 1));
                    sb.append(",");
                    sb.append(values[1]);
                    v = sb.toString();
                    phrase.set(k);
                    word.set(v);
                    context.write(phrase, word);
                }
            }
        }
    }
    
	/**
	 * The Reduce class
	 * @author wudongdong
	 *
	 */
    public static class ngramsReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    	private static Configuration conf;
    	private static int n;
    	
    	/**
    	 * A class store the word and its probability
    	 * @author wudongdong
    	 *
    	 */
    	class Word implements Comparable<Word>{
    		
    		private String word;
    		private int p;
    		
    		public Word(String word, String p){
    			this.word = word;
    			this.p = Integer.parseInt(p);
    		}

    		@Override
    		public int compareTo(Word o) {
    			if(this.p == o.p){
    				return this.word.compareTo(o.word);
    			}
    			return o.p - this.p;
    		}    		
    	}
    	
    	/**
    	 * Setup for this class
    	 * @throws Exception Connection exception
    	 */
    	@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	PriorityQueue<Word> q = new PriorityQueue<Word>();
        	
        	// get the phrase count and store words and their count in the map
        	for(Text value : values){
        		String line = value.toString();
        		String[] temp = line.split(",");
        		if(temp.length == 2){
        			q.offer(new Word(temp[0], temp[1]));
        		}
        	}
        	
        	// calculate the probability
        	Put put = new Put(Bytes.toBytes(key.toString()));
    		int i = 0;
    		boolean flag = false;
    		while(!q.isEmpty() && i < n){
    			i++;
    			Word temp = q.poll();
    			put.add(Bytes.toBytes("data"),Bytes.toBytes(temp.word), Bytes.toBytes(String.valueOf(temp.p)));
    			flag = true;
    		}
    		if(flag){
    			context.write(null, put);
    		}    	
        }
    }
    
    /**
     * The Main function
     * @param args The input and output path
     * @throws Exception IOException
     */
    public static void main(String[] args) throws Exception {
    	
    	Configuration conf = new Configuration();
    	GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        
    	Job job = new Job(conf,"N-Grames");
        job.setJarByClass(Task2.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(ngramstMap.class);
        TableMapReduceUtil.initTableReducerJob("test", ngramsReducer.class, job);               
        
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
        	otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        
        job.waitForCompletion(true);
    }
}
