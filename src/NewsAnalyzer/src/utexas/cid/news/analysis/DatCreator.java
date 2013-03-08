package utexas.cid.news.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import utexas.cid.news.Constants;
import utexas.cid.news.NewsArticle;
import utexas.cid.news.dataservices.NewsDao;

/**
 * For each news article, counts the number
 * of occurrences of each word, creating a "column" of data
 * suitable for input to our MATLAB code
 * 
 * Results are combined into a single matrix for all words,
 * for all news stories
 * 
 * @author Ryan Golden -- ryancgolden@gmail.com
 */
public class DatCreator {

    /** The logger. */
    private static Log log = LogFactory.getLog(DatCreator.class);

    //public static String PEOPLE_XML = File.separator + "govtrack" + File.separator + "people.xml";
    //public static String BY_REP_DIR = File.separator + "govtrack" + File.separator + "byrep";
    public static String WC_DIR = File.separator + "tmp" + File.separator + "news" + File.separator + "dat";
    public static String sRootDir = "";
    
    /**
     * Parse people.xml and break out speech data by rep.
     * @param args - first argument must be absolute dir below which these files/dirs reside:
     * <pre>
     *     govtrack/people.xml
     *     govtrack/cr
     *     govtrack/byrep
     *     govtrack/wc
     * </pre>
     * @throws Exception
     */
 
	private static StringSerializer stringSerializer = StringSerializer.get();

   	/**
	 * XXX: this isn't scalable and cuts off at 2000 cols right now
	 * Though in a week of news scraping, I never found an article with
	 * more than 800 unique terms.
	 * 
	 * XXX: Should page through rows and columns rather than read it all into memory
	 */

    public static void main(String[] args) throws Exception {

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
    			QueryResult<OrderedRows<String, String, Integer>> result = rangeSlicesQuery.execute();
    			OrderedRows<String, String, Integer> orderedRows = result.get();

    			// This for paging, but we're not using it right now
    			Row<String, String, Integer> lastRow = orderedRows.peekLast();


    	    	// Map of terms to an array of int, each int representing how many times the term
    	        // appears for the corresponding doc index
    	        Map<String, Integer[]> myTermDocMat =
    	            new HashMap<String, Integer[]>();
    			
	            List<String> myDocList = new ArrayList<String>();
	            List<String> myDocIdList = new ArrayList<String>();

	            // First get a list of all docs
	            //List<NewsArticle> myDocs = new ArrayList<NewsArticle>();
	            NewsDao myNewsDao = NewsDao.getInstance();
	            List<NewsArticle> myDocs = myNewsDao.readAll();
	            
	            // Now create an map to index those docs by id
	            Map<String, NewsArticle> myDocMap = new HashMap<String, NewsArticle>();
	            for (NewsArticle doc : myDocs) {
	                log.info(doc.title);
	                myDocMap.put(doc.getUrl(), doc);
	            } 	            

    	        // print a column in the matrix for each Cassandra row returned
	            int i = 0;
    	        for (Row<String, String, Integer> r : orderedRows) {
    	        	ColumnSlice<String, Integer> mySlice = r.getColumnSlice();
    	        	List<HColumn<String, Integer>> myCols = mySlice.getColumns();
    	            
    	            // individual doc's word counts
    	            Map<String, Integer> myTermCount = new HashMap<String, Integer>();
    	            
	                myDocList.add(r.getKey());  // Probably we want the title here, not the URL
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
	                    	// initialize the row with zeros, then set the count for this doc
	                        Integer[] counts = new Integer[orderedRows.getCount()];
	                        for (int k=0; k<counts.length; k++) {
	                            counts[k] = 0;
	                        }
	                        counts[i] = myTermCount.get(word);
	                        myTermDocMat.put(word, counts);
	                    }
	                }
    	        	i++;  // keep counter for doc index
    	        }
    	            
    	        // Write wc.dat file, a term-doc matrix (with no headers)
    	        BufferedWriter out = new BufferedWriter(
    	                new FileWriter(sRootDir + File.separator + WC_DIR
    	                        + File.separator + "wc.dat"));   	        
    	        List<String> myWordList =
    	            Arrays.asList(myTermDocMat.keySet().toArray(new String[0]));        
    	        //for (int c=0; c<myWordList.size() && c<MAX_WORDS; c++) {
    	        for (int c=0; c<myWordList.size(); c++) {
    	                String word = myWordList.get(c);
    	            for (Integer count : myTermDocMat.get(word)) {
    	                out.write(count + " ");
    	            }
    	            out.write("\n");
    	        }
    	        out.close();
    	        
    	        // write words.dat
    	        out = new BufferedWriter(
    	                new FileWriter(sRootDir + File.separator + WC_DIR
    	                        + File.separator + "words.dat"));       
    	        
    	        for (int c=0; c<myWordList.size(); c++) {
    	            String term = myWordList.get(c);
    	            out.write("\"" + term + "\"\n");
    	        }
    	        out.close();

    	        // write docs.dat
    	        out = new BufferedWriter(
    	                new FileWriter(sRootDir + File.separator + WC_DIR
    	                        + File.separator + "docs.dat"));       
    	        for (String docId : myDocIdList) {
    	            NewsArticle doc = myDocMap.get(docId);
    	            out.write("\"" + doc.title + "\" ");
    	            out.write("\"" + doc.url + "\" ");
    	            out.write("\"" + doc.author + "\" ");
    	            out.write("\"" + doc.feedUri + "\" ");
//    	            out.write("\"" + doc.description + "\" ");
//    	            out.write("\"" + doc.getPubDateAsString() + "\" ");
//    	            out.write("\"" + p.district + "\" ");
//    	            out.write("\"" + p.gender + "\" ");
//    	            out.write("\"" + p.religion + "\" ");
//    	            out.write("\"" + p.birthday + "\" ");
//    	            out.write("\"" + p.birthyear + "\" ");
    	            out.write("\n");
    	        }
    	        out.close();
    			
    		} catch (HectorException he) {
    			he.printStackTrace();
    		}

    		cluster.getConnectionManager().shutdown();
    		
    		log.info("Done writing *.dat files.");
    }
   
}
