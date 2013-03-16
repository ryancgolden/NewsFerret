package utexas.cid.news.analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utexas.cid.news.Constants;
import utexas.cid.news.NewsArticle;
import utexas.cid.news.dataservices.NewsDao;

/**
 * For each news article, counts the number of occurrences of each word,
 * creating a "column" of data suitable for input to our MATLAB code
 * 
 * Results are combined into a single matrix for all words, for all news stories
 * 
 * @author Ryan Golden -- ryancgolden@gmail.com
 */
public class DatCreator {

	/** The logger. */
	private static Logger logger = LoggerFactory.getLogger(DatCreator.class);

	// this will be appended to the passed-in root directory to
	// contain the output files
	public static String sOutDir = "termdoc";

	private static StringSerializer stringSerializer = StringSerializer.get();

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		createDatFiles(args[0]);
	}

	/**
	 * Write term-doc matrix data files for MATLAB consumption:
	 * 
	 * XXX: this isn't scalable and cuts off at 2000 cols right now Though in a
	 * week of news scraping, I never found an article with more than 800 unique
	 * terms.
	 * 
	 * XXX: Should page through rows and columns rather than read it all into
	 * memory
	 * 
	 * XXX: Should probably break this method up a bit
	 * 
	 * <pre>
	 *     <pRootDir>/news/wc.dat
	 *     <pRootDir>/news/words.dat
	 *     <pRootDir>/news/docs.dat
	 *     <pRootDir>/importantstems.dat
	 * </pre>
	 * 
	 * @param pRootDatDir
	 *            - String absolute dir below which output will be written
	 *            - NOTE: importantwords.txt must also exist here, in order to
	 *            -    to create importantstems.dat
	 * @throws Exception
	 */
	public static void createDatFiles(String pRootDatDir) throws Exception {

		// Create the output directory if it doesn't exist
		if (pRootDatDir == null) {
			throw new Exception("Must pass root directory for output.");
		}
		try {
			File myOutDir = new File(pRootDatDir + File.separator + sOutDir);
			if (!myOutDir.exists()) {
				logger.info("Out dir passed doesn't exist, so creating...");
				boolean result = myOutDir.mkdirs();
				if (result) {
					logger.info("Created " + pRootDatDir);
				} else {
					throw new Exception("Failed creating " + pRootDatDir
							+ File.separator + sOutDir);
				}
			}
		} catch (Exception e) {
			logger.error("Problem with directory argument " + pRootDatDir, e);
			return;
		}

		// Individual input/output files
		String sep = File.separator;
		String myWcDatFile = pRootDatDir + sep + sOutDir + sep + "wc.dat";
		String myWordsDatFile = pRootDatDir + sep + sOutDir + sep + "words.dat";
		String myDocsDatFile = pRootDatDir + sep + sOutDir + sep + "docs.dat";
		String myImportantWordsTxtFile = pRootDatDir + sep + "importantwords.txt";
		String myImportantStemsDatFile = pRootDatDir + sep + "importantstems.dat";
		String myImportantStemsMapDatFile = pRootDatDir + sep + "importantstemsmap.dat";
		String myStopWordsTxtFile = pRootDatDir + sep + "stopwords.txt";
		String myStopStemsDatFile = pRootDatDir + sep + "stopstems.dat";
		String myStopStemsMapDatFile = pRootDatDir + sep + "stopstemsmap.dat";

		// Convert importantwords.txt into importantstems.dat
		// which has the same terms, but stemmed.  This allows importantwords.txt
		// to be maintained as a simple English text file
		createStemsDat(myImportantWordsTxtFile, myImportantStemsDatFile, myImportantStemsMapDatFile);

		// Since we convert importantwords, we also need to convert stopwords.txt
		createStemsDat(myStopWordsTxtFile, myStopStemsDatFile, myStopStemsMapDatFile);
		
		
		// XXX: should not couple to Cassandra here, but 
		// should use NewsDao instead
		int maxCols = 0;
		Cluster cluster = HFactory.getOrCreateCluster(Constants.CLUSTER,
				Constants.HOST_IP);
		Keyspace keyspaceOperator = HFactory.createKeyspace(Constants.KEYSPACE,
				cluster);

		try {
			// Do Cassandra query to get all rows, all columns
			RangeSlicesQuery<String, String, Integer> rangeSlicesQuery = HFactory
					.createRangeSlicesQuery(keyspaceOperator, stringSerializer,
							stringSerializer, IntegerSerializer.get());
			rangeSlicesQuery.setColumnFamily(Constants.DOC_TERM_COLUMN_FAMILY);
			rangeSlicesQuery.setKeys("", "");
			rangeSlicesQuery.setRange("", "", false, 2000);
			rangeSlicesQuery.setRowCount(500);
			QueryResult<OrderedRows<String, String, Integer>> result = rangeSlicesQuery
					.execute();
			OrderedRows<String, String, Integer> orderedRows = result.get();

			// This for paging, but we're not using it right now
			Row<String, String, Integer> lastRow = orderedRows.peekLast();

			// Map of terms to an array of int, each int representing how many
			// times the term appears for the corresponding doc index
			Map<String, Integer[]> myTermDocMat = new HashMap<String, Integer[]>();

			List<String> myDocList = new ArrayList<String>();
			List<String> myDocIdList = new ArrayList<String>();

			// First get a list of all docs
			NewsDao myNewsDao = NewsDao.getInstance();
			List<NewsArticle> myDocs = myNewsDao.readAll();

			// Now create an map to index those docs by id
			Map<String, NewsArticle> myDocMap = new HashMap<String, NewsArticle>();
			for (NewsArticle doc : myDocs) {
				logger.info(doc.title);
				myDocMap.put(doc.getUrl(), doc);
			}

			// print a column in the matrix for each Cassandra row returned
			int i = 0;
			for (Row<String, String, Integer> r : orderedRows) {
				ColumnSlice<String, Integer> mySlice = r.getColumnSlice();
				List<HColumn<String, Integer>> myCols = mySlice.getColumns();

				// individual doc's word counts
				Map<String, Integer> myTermCount = new HashMap<String, Integer>();
				myDocList.add(r.getKey());
				myDocIdList.add(r.getKey());

				// First, get word count for this doc from Cassandra row
				for (HColumn<String, Integer> col : myCols) {
					myTermCount.put(col.getName(), col.getValue());
				}

				// Now add it to the master matrix
				for (String word : myTermCount.keySet()) {
					if (myTermDocMat.containsKey(word)) {
						Integer[] counts = myTermDocMat.get(word);
						counts[i] = myTermCount.get(word);
					} else {
						// initialize the row with zeros, then set the count for
						// this doc
						Integer[] counts = new Integer[orderedRows.getCount()];
						for (int k = 0; k < counts.length; k++) {
							counts[k] = 0;
						}
						counts[i] = myTermCount.get(word);
						myTermDocMat.put(word, counts);
					}
				}
				i++; // keep counter for doc index
			}

			// Write wc.dat file, a term-doc matrix (with no headers)
			BufferedWriter out = new BufferedWriter(new FileWriter(myWcDatFile));
			List<String> myWordList = Arrays.asList(myTermDocMat.keySet()
					.toArray(new String[0]));
			// for (int c=0; c<myWordList.size() && c<MAX_WORDS; c++) {
			for (int c = 0; c < myWordList.size(); c++) {
				String word = myWordList.get(c);
				for (Integer count : myTermDocMat.get(word)) {
					out.write(count + " ");
				}
				out.write("\n");
			}
			out.close();
			logger.info("Done writing " + myWcDatFile);

			// write words.dat
			out = new BufferedWriter(new FileWriter(myWordsDatFile));
			for (int c = 0; c < myWordList.size(); c++) {
				String term = myWordList.get(c);
				out.write("\"" + term + "\"\n");
			}
			out.close();
			logger.info("Done writing " + myWordsDatFile);

			// write docs.dat
			out = new BufferedWriter(new FileWriter(myDocsDatFile));
			for (String docId : myDocIdList) {
				NewsArticle doc = myDocMap.get(docId);
				out.write("\"" + doc.title + "\" ");
				out.write("\"" + doc.url + "\" ");
				out.write("\"" + doc.author + "\" ");
				out.write("\"" + doc.feedUri + "\" ");
				// out.write("\"" + doc.description + "\" ");
				out.write("\n");
			}
			out.close();
			logger.info("Done writing " + myDocsDatFile);

		} catch (HectorException he) {
			logger.error("Hector problem", he);
		}

		cluster.getConnectionManager().shutdown();
		logger.info("Done writing data files.");
	}

	/**
	 * Create stems from important words
	 * @param pWordsTxtFile - location of input txt file
	 * @param pStemsDatFile - output location of stems file
	 * @param pStemsMapDatFile - output location of stem -> [word] map file
	 * @throws FileNotFoundException if input txt not found
	 * @throws IOException - if output file(s) can't be written
	 */
	private static void createStemsDat(String pWordsTxtFile, 
			String pStemsDatFile, String pStemsMapDatFile) throws FileNotFoundException, IOException {

		Scanner sc = null;
		BufferedWriter out = null;
		BufferedWriter outMap = null;
		Map<String, Set<String>> stemMap = new HashMap<String, Set<String>>(); // preserve order

		try {
			// read important words into Map of stem -> {words}
			sc = new Scanner(new File(pWordsTxtFile));
			while (sc.hasNext()) {
				String myWord = sc.next().toLowerCase();
				String myStem = Stemmer.easyStem(myWord);
				if (stemMap.containsKey(myStem)) {
					stemMap.get(myStem).add(myWord);
				} else {
					Set<String> myWordSet = new HashSet<String>();
					myWordSet.add(myWord);
					stemMap.put(myStem, myWordSet);
				}
			}
			
			// Now write the stems and stems map to dat files
			out = new BufferedWriter(new FileWriter(pStemsDatFile));
			outMap = new BufferedWriter(new FileWriter(pStemsMapDatFile));
			Collection<String> unsorted = stemMap.keySet();
			List<String> sorted = DatCreator.asSortedList(unsorted);
			for (String stem : sorted) {
				out.write(stem + "\n");
				outMap.write(stem);
				for (String word : stemMap.get(stem)) {
					outMap.write(" " + word);
				}
				outMap.write("\n");
			}			
			
			logger.info("Done writing " + pStemsDatFile);
			logger.info("Done writing " + pStemsMapDatFile);

		} catch (FileNotFoundException e) {
			logger.error("Problem reading " + pWordsTxtFile, e);
			throw(e);
		} catch (IOException ioe) {
			logger.error("Problem creating stemming output", ioe);
			throw(ioe);			
		} finally {
			if (sc!=null) {
				sc.close();}
			if (out!=null) {
				out.close();}
			if (outMap!=null) {
				outMap.close();}
		}
	}
	
	/**
	 * Utility method to sort a Collection
	 * @param c - Collection to sort
	 * @return sorted List
	 */
	public static
	<T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c) {
	  List<T> list = new ArrayList<T>(c);
	  java.util.Collections.sort(list);
	  return list;
	}

}
