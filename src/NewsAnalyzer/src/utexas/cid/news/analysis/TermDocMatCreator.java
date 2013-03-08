package utexas.cid.news.analysis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utexas.cid.news.Constants;
import utexas.cid.news.dataservices.NewsDao;
/**
 * Implements Hadoop Map-Reduce to take news articles from a Cassandra column
 * family and produce a term-document matrix for further analytical processing.
 * 
 * @author Ryan Golden <ryancgolden@gmail.com>
 */
public class TermDocMatCreator extends Configured implements Tool {

	private static final Logger logger = LoggerFactory
			.getLogger(TermDocMatCreator.class);

	
	/**
	 * Maps <doc, full content> to <doc, term>
	 */
	public static class TokenizerMapper 
		extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text> {

		private Text word = new Text();
		private ByteBuffer sourceColumn;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			sourceColumn = ByteBufferUtil.bytes(context.getConfiguration().get(
					NewsDao.COL_CONTENT));
			logger.debug("sourceColumn = " + context.getConfiguration().get(
					NewsDao.COL_CONTENT));
		}

		public void map(ByteBuffer key,
				SortedMap<ByteBuffer, IColumn> columns,
				Context context)
						throws IOException, InterruptedException {
			logger.debug("In map step for article (URL): " + ByteBufferUtil.string(key) );

			IColumn column = columns.get(sourceColumn);
			if (column == null)
				return;
			String value = ByteBufferUtil.string(column.value());
			

			StringTokenizer itr = new StringTokenizer(value);
			while (itr.hasMoreTokens()) {
				String myStr = itr.nextToken();
				myStr = myStr.replaceAll("[^\\w\\s]", "");
				myStr = myStr.toLowerCase();
				word.set(myStr);
				if ("".equals(myStr)) {
					// for the case that the only thing in the string were the tokens just removed
					//logger.warn("Found empty string");
					continue;
				}
				context.write(new Text(ByteBufferUtil.string(key)), word);
			}
		}
	}

    public static class ReducerToCassandra extends Reducer<Text, Text, ByteBuffer, List<Mutation>>
    {
        public void reduce(Text doc, Iterable<Text> terms, Context context) throws IOException, InterruptedException
        {
        	// First create a map of total counts for each term
            Map<String, Integer> myTermCountMap = new HashMap<String, Integer>();
            for (Text term : terms) {
                if (myTermCountMap.containsKey(term.toString())) {
                	// Increment the counter for this word
                	myTermCountMap.put(term.toString(), (myTermCountMap.get(term.toString()))+1);
                } else {
                	myTermCountMap.put(term.toString(), 1);
                }
            }

            // Now write ColumnFamily[doc][term]=count
            for (String termStr : myTermCountMap.keySet()) {
            	Mutation myMutation = getMutation(termStr, myTermCountMap.get(termStr));
            	context.write(ByteBufferUtil.bytes(doc.toString()), Collections.singletonList(myMutation));
            }

        }

        /**
         * Create a Cassandra mutation operation (essentially a create or update
         * of a name-value pair in a row, i.e., column).
         * 
         * @param name - key
         * @param val - value
         * @return Mutation
         */
        private static Mutation getMutation(String name, Integer val)
        {
            Column c = new Column();
            c.setName(name.getBytes());
            c.setValue(ByteBufferUtil.bytes(val));
            c.setTimestamp(System.currentTimeMillis());

            Mutation m = new Mutation();
            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
            m.column_or_supercolumn.setColumn(c);
            return m;
        }
    }    
	
	/**
	 * Parse input args, create and run hadoop job
	 */
	@Override
	public int run(String[] args) throws Exception {

		logger.debug("In run method.");

		// TODO: optionally get these from command line
		String myCasPort = Constants.PORT;
		String myCasHost = Constants.HOST;

		String columnName = NewsDao.COL_CONTENT;
		getConf().set(NewsDao.COL_CONTENT, columnName);

		Job job = new Job(getConf(), "tdm-creator");
		job.setJarByClass(TermDocMatCreator.class);

		// Mapper
		job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        // Reducer
        job.setReducerClass(ReducerToCassandra.class);
        job.setOutputKeyClass(ByteBuffer.class);
        job.setOutputValueClass(List.class);
        
        // OutputFormat
        job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), Constants.KEYSPACE,
        		Constants.DOC_TERM_COLUMN_FAMILY);

		// InputFormat
		job.setInputFormatClass(ColumnFamilyInputFormat.class);
		ConfigHelper.setRpcPort(job.getConfiguration(), myCasPort);
		ConfigHelper.setInitialAddress(job.getConfiguration(), myCasHost);
		ConfigHelper.setPartitioner(job.getConfiguration(),	"org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), Constants.KEYSPACE, Constants.NEWS_COLUMN_FAMILY);
		SlicePredicate predicate = new SlicePredicate()
				.setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
		
//		job.setCombinerClass(ReducerToFilesystem.class);
//		job.setReducerClass(ReducerToFilesystem.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(MapWritable.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
//		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));

		job.waitForCompletion(true);

		return 0;

	}
	
	/**
	 * Run map reduce against News to turn content column into a term-doc matrix
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		ToolRunner.run(new Configuration(), new TermDocMatCreator(), args);
		System.exit(0);
	}

}
