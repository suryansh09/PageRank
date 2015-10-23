
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankIterSort {
	public static class PageRankIterSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration() ;
			long count = Long.parseLong(conf.get("Count")) ;
			double valueToCompare = (double) (5.0/count) ;
			int index = value.find("\t") ;
			String titleKey = Text.decode(value.getBytes(), 0, index) ;
			int indexRank = value.find("\t", index + 1) ;
			String intialRank = null ;
			if (indexRank != -1)
				intialRank = Text.decode(value.getBytes(), index+1, indexRank-(index+1)) ;
			else
				intialRank = Text.decode(value.getBytes(), index+1, value.getLength()-(index+1)) ;

			double newRank = Double.parseDouble(intialRank) ;

			if (newRank < valueToCompare) return ;
			Text title = new Text(titleKey) ;
			context.write(new DoubleWritable(newRank), title);
		}
	}

	public static class PageRankIterSortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			while (it.hasNext())
				context.write(it.next(), key);
		}
	}

	public static class SortDoubleComparator extends WritableComparator {
		public SortDoubleComparator() {
			super(DoubleWritable.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable k1 = (DoubleWritable)w1;
			DoubleWritable k2 = (DoubleWritable)w2;
			return -1 * k1.compareTo(k2);
		}
	}

	public void pageRankSort(String count, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration() ;
		conf.set("Count", count) ;
		@SuppressWarnings( "deprecation" )
		Job job = new Job(conf, "PageRankIterSort") ;
		job.setJarByClass(PageRankIterSort.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(PageRankIterSortMapper.class);
		job.setReducerClass(PageRankIterSortReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setSortComparatorClass(SortDoubleComparator.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
} 


