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
	
public class votes {
		
	public static class Map extends Mapper<LongWritable , Text, Text, IntWritable> {
	private List<String> positive_Words = new ArrayList<String>();
        {
            positive_Words.add("excellent");
            positive_Words.add("great");
            positive_Words.add("love");
            positive_Words.add("like");
	    positive_Words.add("awesome");
	    positive_Words.add("super");
        }
	private List<String> negative_Words = new ArrayList<String>();
        {
            negative_Words.add("bad");
            negative_Words.add("not");
            negative_Words.add("love");
            negative_Words.add("overpriced");
	    negative_Words.add("bad");
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] tokens = value.toString().split("\t");
		String keyval;
		int flag=1;
		String total_votes = tokens[9];
		String product_category = tokens[6];
		String product_id = tokens[3];
		String review_body = tokens[13];
		keyval = product_category+ ","+total_votes;
		for (String s : positive_Words) {
       			if(review_body.contains(s)){
				flag=0;
				Text str = new Text(keyval + "   Positive Score =");
           			context.write(str, new IntWritable(1));
           			break;
       			}
    		}
		for (String s : negative_Words) {
       			if(review_body.contains(s) && flag==1){
				Text str1 = new Text(keyval + "   Negative Score =");
           			context.write(str1, new IntWritable(-1));
           			break;
       			}
    		}

        }
    }
	
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    } 
	

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
        // Job 1
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(votes.class);
        job.setMapperClass(Map.class);

        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

}
}
