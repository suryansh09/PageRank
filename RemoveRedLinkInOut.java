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

public class RemoveRedLinkInOut {
	public static class RemoveRedLinkMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int titleIndex = value.find("\t") ;
			if (titleIndex == -1) {
				context.write(new Text(value.toString()), new Text("!")) ;
			}
			else {
				String title = Text.decode(value.getBytes(),0,titleIndex) ;
				Text titleKey = new Text(title) ;
				context.write(titleKey, new Text("!"));
				String chainLinks = Text.decode(value.getBytes(), titleIndex+1, value.getLength()-(titleIndex+1)) ;
				String[] completeLinks = chainLinks.split("\t") ;
				for (String links: completeLinks){
					links = links.trim() ;
					if (links != null)
						if (!links.isEmpty())
							context.write(new Text(links), titleKey);
				}
			}
		}
	}

	public static class RemoveRedLinkReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String iteratorValue ;
			String links = "" ;
			boolean isThere = false ;
			boolean first = true ;
			Iterator<Text> it = values.iterator() ;
			while(it.hasNext()) {
				iteratorValue = it.next().toString() ;

				if (iteratorValue.equals("!"))
					isThere = true ;
				else {
					if (!first)
						links = links+"\t"+iteratorValue ;
					else {
						links = links+iteratorValue ;
						first = false ;
					}
				}
			}
			if (isThere)
				context.write(key, new Text(links)) ;
		}
	}

	public void removeRedLink(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration() ;
		@SuppressWarnings( "deprecation" )
		Job job = new Job(conf, "redlink1") ;
		job.setJarByClass(RemoveRedLinkInOut.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(RemoveRedLinkMapper.class);
		job.setReducerClass(RemoveRedLinkReducer.class) ;
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
} 

