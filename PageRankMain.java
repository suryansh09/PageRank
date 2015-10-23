import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;


public class PageRankMain {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		String input = args[0];
		String output = args[1];
		String outputResult = args[1];
		output += "/wiki";
		XMLToGraph graphGen = new XMLToGraph() ;
		graphGen.XMLParsing(input, output + "/ranking/temp/");
		// Generate In Graph
		RemoveRedLinkInOut remover = new RemoveRedLinkInOut() ;
		remover.removeRedLink(output + "/ranking/temp", output + "/ranking/iter0--‐raw/");
		// Generate Out Graph
		remover.removeRedLink(output + "/ranking/iter0--‐raw", output + "/ranking/iter0/");
		CountLinks counter = new CountLinks() ;
		// Count Total Number
		counter.countTotalLinks(output + "/ranking/iter0", output + "/ranking/N/");
		String countPath = output + "/ranking/N" + "/part-r-00000" ;
		Configuration conf = new Configuration() ;
		Path pt = new Path(countPath) ;
		FileSystem fs = FileSystem.get(new URI(countPath), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt))) ;
		String line = br.readLine() ;
		String count = line.substring(2) ;
		DefaultIterPageRankGraph iterPage = new DefaultIterPageRankGraph() ;
		iterPage.IterPageRankGraph(count, output + "/ranking/iter0", output + "/ranking/itertemp/");
		// Rank Calculation
		CalculatePageRank calculateRank = new CalculatePageRank() ;
		calculateRank.calculateRank(output + "/ranking/itertemp", output + "/ranking/iter" + 1 + "/", count);
		int iterations = 8;
		for(int run = 1; run<iterations;run++)
			calculateRank.calculateRank(output + "/ranking/iter" + run, output + "/ranking/iter" + (run + 1) + "/", count);
		PageRankIterSort pageRankCalculator = new PageRankIterSort() ;
		pageRankCalculator.pageRankSort(count, output + "/ranking/iter" + 1, output + "/ranking/sort1/");
		pageRankCalculator.pageRankSort(count, output + "/ranking/iter" + 8, output + "/ranking/sort2/");
		finalResult(new Path(output + "/ranking/iter0/part-r-00000"), new Path(outputResult + "/result/PageRank.outlink.out"), conf);
		finalResult(new Path(output + "/ranking/N/part-r-00000"), new Path(outputResult + "/result/PageRank.n.out"), conf);
		finalResult(new Path(output + "/ranking/sort1/part-r-00000"), new Path(outputResult + "/result/PageRank.iter1.out"), conf);
		finalResult(new Path(output + "/ranking/sort2/part-r-00000"), new Path(outputResult + "/result/PageRank.iter8.out"), conf);
	}

	private static void finalResult(Path source, Path destination, Configuration conf) throws IOException {
		FileSystem getSource = source.getFileSystem(conf);
		FileSystem getDestination = destination.getFileSystem(conf);
		FileUtil.copy(getSource, source, getDestination, destination, false, conf);
	}

}
