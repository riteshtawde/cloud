package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

// each map task handles one line within an adjacency matrix file
// key: file offset
// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			//Log log = LogFactory.getLog(PageRankMap.class);
			int numUrls = context.getConfiguration().getInt("numUrls",1);
			String line = value.toString();
			StringBuffer sb = new StringBuffer();
			// instance an object that records the information for one webpage
			RankRecord rrd = new RankRecord(line);
			int sourceUrl, targetUrl;
			// double rankValueOfSrcUrl;
			//log.info("********************************************");
			//log.info(" Source URL-> "+rrd.sourceUrl);
			if (rrd.targetUrlsList.size()<=0){
				// there is no out degree for this webpage; 
				// scatter its rank value to all other urls
				double rankValuePerUrl = rrd.rankValue/(double)numUrls;
				for (int i=0;i<numUrls;i++){
					context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
				}
			} else {
				/*Write your code here*/
				// if a sourceUrl has outgoing links, then divide its rank equally among its destination URl
				double rankValuePerDestinationURL = rrd.rankValue/(double)rrd.targetUrlsList.size();
				System.out.println("RankValuePerDestinationURL-> "+rankValuePerDestinationURL);
				for(int i:rrd.targetUrlsList){
					System.out.println("Source URL-> "+rrd.sourceUrl+" ..targetURLS-> "+i);
					context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerDestinationURL)));
				}
				for (int i=0;i<rrd.targetUrlsList.size();i++) {
					targetUrl = rrd.targetUrlsList.get(i);
					sb.append("#"+targetUrl);
				}
				//log.info(sb.toString());
			} //for			
			
			//log.info("********************************************");
			context.write(new LongWritable(rrd.sourceUrl), new Text(sb.toString()));
		} // end map							

}