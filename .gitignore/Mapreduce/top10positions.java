


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



 public class top10positions {
		public static class Top10Mapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
				public void map(LongWritable key, Text value, Context context
		        ) throws IOException, InterruptedException {
					final IntWritable one= new IntWritable(1);
					try {
					String[] str = value.toString().split("\t");
					String position= str[4];
					int year= Integer.parseInt(str[7]);
					String status= str[1];
					String myyear= String.format("%d", year);
					String myvalue= position + ',' + status + ',' + myyear;
					context.write(new Text(myvalue),one);
			           
		         }
		         catch(Exception e)
		         {
		            System.out.println(e.getMessage());
		         }
		      }
		   }
		public static class Top10Reducer extends
		Reducer<Text, IntWritable, NullWritable, Text> {
			private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

			public void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {
				long count=0;
				String myvalue= "";
				String mycount= "";
					for (IntWritable val : values) {
					//String[] token= key.toString().split(",");
					 count++;
					}
					myvalue= key.toString();
					mycount= String.format("%d", count);
					myvalue= myvalue + ',' + mycount;
					repToRecordMap.put(new Integer(mycount), new Text(myvalue));
					if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
							}
			}
			
					protected void cleanup(Context context) throws IOException,
					InterruptedException {
					for (Text t : repToRecordMap.descendingMap().values()) {
						// Output our five records to the file system with a null key
						context.write(NullWritable.get(), t);
						}
					}
		}
public static void main(String[] args) throws Exception {
			
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Top 10 positions");
		    job.setJarByClass(top10positions.class);
		    job.setMapperClass(Top10Mapper.class);
		    job.setReducerClass(Top10Reducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}

