import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	
	public class WordCount {
	static int count=0;
	   public static class Map extends Mapper<Object, Text, Text, IntWritable> {
	     //private final static IntWritable one = new IntWritable(1);
	     private Text word = new Text();
	//final LongWritable key=new LongWritable();
	     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	       String[] tokens = value.toString().split("\t");
		count++;
		String keyval;
        	String product_id = tokens[3];
        	String star_rating = tokens[7];
		String product_category = tokens[6];
		
		if(star_rating.equals("5"))
		{
        	 //keyval = product_id + "," + star_rating+"," + product_category;
		keyval = product_category + "," + star_rating;
		word.set(keyval);

        	context.write(word, new IntWritable(1));
		}
	       }
	     }
	   
	
	       public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	       int sum = 0;

	       for (IntWritable val : values) {
                sum += val.get();
            }
		final int average = (sum *100) / count;
		Text str = new Text("Average for Category "+ key+" Rating (in %) = ");
            context.write(str, new IntWritable(average));
	   }
	}


	   public static void main(String[] args) throws Exception {
	    
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(WordCount.class);
        //job.setJar("WordCount2.jar");
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
	job.waitForCompletion(true);
	   }
	}
