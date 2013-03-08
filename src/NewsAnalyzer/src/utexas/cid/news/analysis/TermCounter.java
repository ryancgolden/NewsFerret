package utexas.cid.news.analysis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utexas.cid.news.Constants;
import utexas.cid.news.dataservices.NewsDao;
/**
 * Implements Hadoop Map-Reduce to take news articles from a Cassandra column
 * family and produce a term count across docs for further analytical processing.
 * 
 * @author Ryan Golden <ryancgolden@gmail.com>
 */
public class TermCounter extends Configured implements Tool {

	private static final Logger logger = LoggerFactory
			.getLogger(TermCounter.class);

    // filesystem output location
    private static final String OUTPUT_PATH_PREFIX = "/tmp/news/termcount";

	/**
	 * Run map reduce against News to turn content column into a term-doc matrix
	 * 
	 * @param args
	 *            - TODO
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		ToolRunner.run(new Configuration(), new TermCounter(), args);
		
		//StatPrinter.printStats();
		
		System.exit(0);
	}

	/**
	 * Mapper takes a Cassandra row as input and outputs <term, 1>
	 * @author rgolden
	 *
	 */
	public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private ByteBuffer sourceColumn;

		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
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
			IColumn column = columns.get(sourceColumn);
			if (column == null)
				return;
			String value = ByteBufferUtil.string(column.value());
			
			logger.debug("map for article: " + ByteBufferUtil.string(key) );

			StringTokenizer itr = new StringTokenizer(value);
			while (itr.hasMoreTokens()) {
				String myStr = itr.nextToken();
				myStr = myStr.replaceAll("[^\\w\\s]", "");
				myStr = myStr.toLowerCase();
				word.set(myStr);
				context.write(word, one);
			}
		}
	}

	/**
	 * Reducer creates file with "<term> <count>" showing the total count of that term
	 * in the entire corpus across all news articles.
	 */
    public static class ReducerToFilesystem extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
            //logger.debug("writing key:" + key.toString() + " value: " + sum);
        }
    }

    /*
    public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>>
    {
        private ByteBuffer outputKey;

        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException
        {
            outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
        }

        private static Mutation getMutation(Text word, int sum)
        {
            Column c = new Column();
            c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
            c.setValue(ByteBufferUtil.bytes(String.valueOf(sum)));
            c.setTimestamp(System.currentTimeMillis());

            Mutation m = new Mutation();
            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
            m.column_or_supercolumn.setColumn(c);
            return m;
        }
    }
*/
	
	
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

		Job job = new Job(getConf(), "TermCounter");
		job.setJarByClass(TermCounter.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(ReducerToFilesystem.class);
		job.setReducerClass(ReducerToFilesystem.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
		job.setInputFormatClass(ColumnFamilyInputFormat.class);

		ConfigHelper.setRpcPort(job.getConfiguration(), myCasPort);
		ConfigHelper.setInitialAddress(job.getConfiguration(), myCasHost);
		ConfigHelper.setPartitioner(job.getConfiguration(),
				"org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), Constants.KEYSPACE,
				Constants.NEWS_COLUMN_FAMILY);
		SlicePredicate predicate = new SlicePredicate()
				.setColumn_names(Arrays.asList(ByteBufferUtil
						.bytes(columnName)));
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(),
				predicate);

		job.waitForCompletion(true);

		return 0;

	}

}
