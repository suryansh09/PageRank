
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CountLinks {
	public static class CountLinksMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private final static Text linkCounts = new Text("counts") ;
		private final static LongWritable count = new LongWritable(1) ;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(linkCounts, count);
		}
	}

	public static class CountLinksReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long number = 0 ;
			for (Iterator<LongWritable> it = values.iterator(); it.hasNext();) {
				it.next();
				number += 1 ;
			}
			context.write(new Text("N"), new LongWritable(number));
		}
	}

	public void countTotalLinks(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration() ;
		conf.set("mapred.textoutputformat.separator", "=");
		@SuppressWarnings( "deprecation" )
		Job job = new Job(conf, "CountLinks") ;
		job.setJarByClass(CountLinks.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(CountLinksMapper.class);
		job.setReducerClass(CountLinksReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);

	}
}
