
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


public class top5employer {
			public static class Top5Mapper extends
				Mapper<LongWritable, Text, Text, Text> {
					public void map(LongWritable key, Text value, Context context
			        ) throws IOException, InterruptedException {
						
						try {
						String[] str = value.toString().split("\t");
						String employer_name= str[2];
						int year= Integer.parseInt(str[7]);
						String status= str[1];
						String myyear= String.format("%d", year);
						String myvalue= employer_name + ',' + myyear;
						context.write(new Text(myvalue),new Text (status));
				           
			         }
			         catch(Exception e)
			         {
			            System.out.println(e.getMessage());
			         }
			      }
			   }
			public static class Top5Reducer extends
			Reducer<Text, Text, NullWritable, Text> {
				private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

				public void reduce(Text key, Iterable<Text> values,
						Context context) throws IOException, InterruptedException {
					long count=0;
					String myvalue= "";
					String mycount= "";
						for (Text val : values) {
						//String[] token= key.toString().split(",");
						 count++;
						}
						myvalue= key.toString();
						mycount= String.format("%d", count);
						myvalue= myvalue + ',' + mycount;
						repToRecordMap.put(new Long(mycount), new Text(myvalue));
						if (repToRecordMap.size() > 5) {
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
			    Job job = Job.getInstance(conf, "Top 5 employer");
			    job.setJarByClass(top5employer.class);
			    job.setMapperClass(Top5Mapper.class);
			    job.setReducerClass(Top5Reducer.class);
			    job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(Text.class);
			    job.setOutputKeyClass(NullWritable.class);
			    job.setOutputValueClass(Text.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
		}

