
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class countengineer {

	public static class datamap extends Mapper<LongWritable,Text,Text,Text> {
		
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException {
			String[] word = value.toString().split("\t");
			String word1 = word[8] +','+ word[7];
			if(word[4].contains("DATA ENGINEER"))
			{		
			
			context.write(new Text(word1), new Text (word[4]));
				}
			}
	}
	public static class datared extends Reducer<Text,Text,NullWritable,Text>
	{
		private TreeMap<Long,Text> repToRecordMap= new TreeMap<Long,Text>();
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			long count=0;
			String myvalue= "";
			for(Text val:values)
			{
				count++;
			}
			myvalue= key.toString();
			String mycount= String.format("%d", count);
			myvalue= myvalue + ',' + mycount;
			repToRecordMap.put(new Long(mycount), new Text(myvalue));
			if(repToRecordMap.size() > 10) 
			{
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
			
		}
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for(Text t: repToRecordMap.descendingMap().values())
			{
				context.write(NullWritable.get(), t);
			}
		}
	}
		
		public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "data count");
		job.setJarByClass(countengineer.class);
		job.setMapperClass(datamap.class);
		job.setReducerClass(datared.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 :1);
		
		

	}
	}
	
	


	
