package utexas.cid.news.analysis;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import utexas.cid.news.dataservices.NewsSchema;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
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

		// Part of speech tagger
		private static String sTaggerLoc = "taggers/english-bidirectional-distsim.tagger";
		private static MaxentTagger sTagger = null;

		private Text word = new Text();
		private ByteBuffer sourceColumn;

		// initialize tagger in static block to save startup time on each task
		// XXX: should we be passing this in the distributed cache?
		//--------------REMOVING TAGGING FOR NOW
//		static {
//			// Create part of speech tagger
//			try {
//				sTagger = new MaxentTagger(sTaggerLoc);
//			} catch (Exception e) {
//				logger.error("Error occurred creating tagger.  Please ensure "
//						+ "training files are accessible at "
//						+ "taggers/english-bidirectional-distsim.tagger", e);
//				//suppress remaining exception... not sure if this is okay
//			}			
//		}
		
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

			//--------------REMOVING TAGGING FOR NOW
			// tagged string contains part of speech suffix
			// see http://www.computing.dcu.ie/~acahill/tagset.html
			//String tagged = sTagger.tagString(value);
			//StringTokenizer itr = new StringTokenizer(tagged);
			StringTokenizer itr = new StringTokenizer(value);
			boolean hasWords = false; //track whether the doc has any valid words
			while (itr.hasMoreTokens()) {
				//String myTaggedStr = itr.nextToken();
				String myRoot = itr.nextToken();
				
				// Remove anything not alphanumeric or underscore
				//--------------REMOVING TAGGING FOR NOW
//				myTaggedStr = myTaggedStr.replaceAll("[^\\w\\s_]", "");
				myRoot = myRoot.replaceAll("[^\\w\\s_]", "");

				//--------------REMOVING TAGGING FOR NOW
//				if (myTaggedStr.length() < 2) {
//					continue;
//				}

				if (myRoot.length() < 2) {
					continue;
				}

//--------------REMOVING TAGGING FOR NOW
//				// Separate root string and tag suffix
//				String myRoot = null;
//				String myTag = null;
//				try {
//					int idx = myTaggedStr.lastIndexOf('_');
//					myRoot = myTaggedStr.substring(0, idx);
//					myTag = myTaggedStr.substring(idx + 1, myTaggedStr.length() - 1);
//				} catch (Exception e) {
//					logger.debug("Problem tagging", e);
//				}

				myRoot = myRoot.toLowerCase();

				// If the string was non alpha, skip it
				// If the string is not a noun, skip it
				//--------------REMOVING TAGGING FOR NOW
//				if ("".equals(myRoot) || myTag == null || !(myTag.contains("NN"))) {
//					continue;
//				}
				if ("".equals(myRoot)) {
					continue;
				}
				
				// Stem the word (Porter stemmer algorithm)
				myRoot = Stemmer.easyStem(myRoot);

				word.set(myRoot);
				context.write(new Text(ByteBufferUtil.string(key)), word);
				hasWords = true;
			}
			if (!hasWords) {
				logger.warn("Document had no valid terms for indexing." );
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

		logger.debug("Creating M-R job to create term doc matrix.");

		// drop and re-create the column family that will hold
		// the output of the M-R job, which is essentially a form of
		// term-doc matrix.
		NewsSchema.dropDocTermColumnFamily();
		NewsSchema.createDocTermColumnFamily();
		
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
