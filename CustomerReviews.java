import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.StringTokenizer;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter.DEFAULT;
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
	
	public class CustomerReviews {
	//static int count=0;
	   public static class Map extends Mapper<Object, Text, Text, IntWritable> {
	     //private final static IntWritable one = new IntWritable(1);
	     private Text word = new Text();
	//final LongWritable key=new LongWritable();
	     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	       String[] tokens = value.toString().split("\t");
		//count++;
		String keyval;
        	String customer_id = tokens[1];
        	//String star_rating = tokens[7];
		String product_category = tokens[6];
		
		if(product_category.equals("product_category")){}
		//{
        	 //keyval = product_id + "," + star_rating+"," + product_category;
        //for(String word: customer_id )
        //{	
		else{
		keyval = product_category + "," + customer_id;
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
		//final int average = (sum *100) / count;
		//Text str = new Text("Lowest rating average in % for "+ key);
		if(sum < 100){}
		else{
            context.write(key, new IntWritable(sum));
}
	   }
	}


	   public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
	    
        Configuration conf = new Configuration();
        Job job = new Job(conf, "CustomerReviews");
        job.setJarByClass(CustomerReviews.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	   }
	}
