
import java.io.IOException;
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

public class CalculatePageRank {

	public static class CalculatePageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keys = value.toString().split("\t");
			String name = keys[0];
			Configuration conf = context.getConfiguration();
			double N = Double.parseDouble(conf.get("Count"));
			double d = 0.85;
			double intialRank = (1-d)/N;
			context.write(new Text(name), new Text(Double.toString(intialRank)));
			double oldWeight = Double.parseDouble(keys[1]);
			if(keys.length==2)
				return;
			String links = "";
			for(int i =2;i<keys.length;i++){
				links+= keys[i] + "\t";
			}
			context.write(new Text(name), new Text("!" + links));
			String[] finalLinks = links.split("\t");
			int numberOfOutLinks = finalLinks.length;
			intialRank = d*(oldWeight/numberOfOutLinks);
			for(int i=0; i<numberOfOutLinks; i++) {
				context.write(new Text(finalLinks[i]), new Text(Double.toString(intialRank)));
			}
		}
	}

	public static class CalculatePageRankReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double initialRank = 0.0;
			String finalLinks = "";
			for(Text value : values) {
				String start = value.toString();
				if(start.startsWith("!")) {
					finalLinks = start.substring(1);
				}
				else {
					initialRank += Double.parseDouble(start);
				}
			}
			String finalValue = (Double.toString(initialRank)+"\t"+finalLinks).trim();
			context.write(key, new Text(finalValue));
		}
	}

	public void calculateRank(String inputPath, String outputPath, String count) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration() ;
		conf.set("Count", count) ;
		@SuppressWarnings( "deprecation" )
		Job job = new Job(conf, "CalculatePageRank") ;
		job.setJarByClass(CalculatePageRank.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CalculatePageRankMapper.class);
		job.setReducerClass(CalculatePageRankReducer.class) ;
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
} 
