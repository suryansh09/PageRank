import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class XMLToGraph {
	public  static class XMLParsingMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final Pattern patterLinks = Pattern.compile("\\[.+?\\]") ;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int start = value.find("<title>") ;
			int end = value.find("</title>", start) ;
			if (start == -1 || end == -1) return ;
			start += 7 ;
			String title = Text.decode(value.getBytes(), start, end-start) ;
			title = title.replace(' ', '_') ;
			Text titleKey = new Text(title) ;
			String outLinks = "" ;
			start = value.find("<text") ;
			if (start == -1) {
				context.write(titleKey, new Text(outLinks));
				return ;
			}
			start = value.find(">", start) ;
			if (start == -1) {
				context.write(titleKey, new Text(outLinks));
				return ;
			}
			end = value.find("</text>") ;
			if (end == -1) {
				context.write(titleKey, new Text(outLinks));
				return ;
			}
			start += 1 ;
			String text = Text.decode(value.getBytes(), start, end-start) ;
			Matcher wikiLinksMatcher = patterLinks.matcher(text) ;
			LinkedList<String> duplicateRemover = new LinkedList<String>()  ;
			while (wikiLinksMatcher.find()){
				String outLinkPage = wikiLinksMatcher.group() ;
				outLinkPage = linksCatcher(outLinkPage) ;
				if (outLinkPage != null){
					if (!outLinkPage.isEmpty()){
						outLinkPage = outLinkPage.trim() ;
						duplicateRemover.add(outLinkPage) ;
					}
				}
			}
			LinkedHashSet<String> duplicatePruning = new LinkedHashSet<String>(duplicateRemover) ;
			LinkedList<String> finalList = new LinkedList<String>(duplicatePruning) ;
			boolean first = true ;
			for (String values: finalList){
				if (!values.equals(title)) {
					if (!first)
						outLinks += "\t" ;
					outLinks += values ;
					first = false ;
				}
			}
			context.write(titleKey, new Text(outLinks));
		}
		private String linksCatcher(String linkPage){
			if (linkChecker(linkPage)) return null ;
			int start = 1 ;
			if(linkPage.startsWith("[["))
				start = 2 ;
			int end = linkPage.indexOf("]") ;
			int position = linkPage.indexOf("|") ;
			if (position > 0){
				end = position ;
			}
			linkPage = linkPage.substring(start, end) ;
			linkPage = linkPage.replaceAll("\\s", "_") ;
			return linkPage ;
		}
		private boolean linkChecker(String outLink) {
			int start = 1;
			if(outLink.startsWith("[[")){
				start = 2;
			}
			if( outLink.length() < start+2 || outLink.length() > 100) return true;
			char charFirst = outLink.charAt(start);
			switch(charFirst){
				case '#': return true;
				case ',': return true;
				case '.': return true;
				case '&': return true;
				case '\'':return true;
				case '-': return true;
				case '{': return true;
			}
			if( outLink.contains(":")) return true;
			if( outLink.contains(",")) return true;
			if( outLink.contains("&")) return true;
			return false;
		}
	}

	public void XMLParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration() ;
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;
		@SuppressWarnings( "deprecation" )
		Job job = new Job(conf, "XMLToGraph") ;
		job.setJarByClass(XMLToGraph.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(inputPath)) ;
		job.setInputFormatClass(XmlInputFormat.class) ;
		job.setMapperClass(XMLParsingMapper.class) ;
		FileOutputFormat.setOutputPath(job, new Path(outputPath)) ;
		job.setOutputFormatClass(TextOutputFormat.class) ;
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}
