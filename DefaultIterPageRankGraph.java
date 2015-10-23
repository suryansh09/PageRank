
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DefaultIterPageRankGraph {
	public static class DefaultIterPageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration() ;
			long count = Long.parseLong(conf.get("Count")) ;
			double previousRank = (double) (1.0/count) ;
			String lastRank = String.valueOf(previousRank);
			int index = value.find("\t") ;
			String title = Text.decode(value.getBytes(),0,index) ;
			String outLinks = Text.decode(value.getBytes(),index+1, value.getLength()-(index+1)) ;
			context.write(new Text(title + "\t" + lastRank), new Text(outLinks));
		}
	}

	public void IterPageRankGraph(String count, String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration() ;
		conf.set("Count", count) ;
		@SuppressWarnings( "deprecation" )
		Job job = new Job(conf, "iter0Graph") ;
		job.setJarByClass(DefaultIterPageRankGraph.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(DefaultIterPageRankMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.waitForCompletion(true);
	}

}
