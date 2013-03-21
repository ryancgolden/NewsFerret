/**
 * Copyright (c) 2013, Ryan Golden. 
 */
package utexas.cid.news.dataservices;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utexas.cid.news.Constants;
import utexas.cid.news.DocTermCount;
import utexas.cid.news.NewsArticle;

/**
 * CRUD like services for identity news items
 * @author Ryan Golden
 *
 */
public class DocTermDao {
	
	private static final Logger logger = LoggerFactory
			.getLogger(DocTermDao.class);

	// Column names
//	public static final String COL_URL = "url";
//	public static final String COL_AUTHOR = "author";
//	public static final String COL_PUB_DATE = "pubdate";
//	public static final String COL_FEED_URI = "feeduri";
//	public static final String COL_DESCRIPTION = "description";
//	public static final String COL_TITLE = "title";
//	public static final String COL_CONTENT = "content";
//	public static final String COL_UPDATE_TIME = "updatetime";
	
	private static DocTermDao sInstance = null;
	private static Keyspace sKeyspace = null;
	private static ColumnFamilyTemplate<String, String> sTemplate = null;
	
	/**
	 * Use static factory method to get instance
	 */
	private DocTermDao() {
		Cluster myCluster = HFactory.getOrCreateCluster(Constants.CLUSTER, Constants.HOST_IP);
		KeyspaceDefinition keyspaceDef = null;
		if (myCluster != null) {
			keyspaceDef = myCluster.describeKeyspace(Constants.KEYSPACE);
		}

//		// If keyspace does not exist, the CF's don't exist either. => create them
//		if (keyspaceDef == null && myCluster != null) {
//			NewsSchema.createNewsColumnFamily(myCluster);
//		}
		
		sKeyspace = HFactory.createKeyspace(Constants.KEYSPACE, myCluster);
		sTemplate =	new ThriftColumnFamilyTemplate<String, String> (
				sKeyspace,
				Constants.DOC_TERM_COLUMN_FAMILY, 
				StringSerializer.get(), 
				StringSerializer.get());
	}
	
	/**
	 * Public static factory method to get instance.  Use this to get a DocTermDao
	 * instance
	 * @return DocTermDao instance
	 */
	public static DocTermDao getInstance() {
		if (sInstance != null) {
			return sInstance;
		}
		sInstance = new DocTermDao();
		return sInstance;
	}
	
	
	/**
	 * Insert, retrieve, and delete a test record
	 * @param args
	 */
	public static void main(String[] args) {
		DocTermDao myDao = DocTermDao.getInstance();
		myDao.readAll();
		
	}

	private static StringSerializer stringSerializer = StringSerializer.get();

	/**
	 * Return a List of all DocTermCounts
	 * XXX: this is not scalable right now... just reading into a giant memory structure
	 * @return List of DocTermCount
	 */
	public List<DocTermCount> readAll() {
		logger.debug("Reading all from DocTermDao");

		List<DocTermCount> myDocTerms = new LinkedList<DocTermCount>();
				
		// Do Cassandra query to get all rows, all columns
		RangeSlicesQuery<String, String, Integer> rangeSlicesQuery = HFactory
				.createRangeSlicesQuery(sKeyspace, stringSerializer,
						stringSerializer, IntegerSerializer.get());
		rangeSlicesQuery.setColumnFamily(Constants.DOC_TERM_COLUMN_FAMILY);
		rangeSlicesQuery.setKeys("", "");
		rangeSlicesQuery.setRange("", "", false, 2000);
		rangeSlicesQuery.setRowCount(500);
		QueryResult<OrderedRows<String, String, Integer>> result = rangeSlicesQuery
				.execute();
	
		OrderedRows<String, String, Integer> rows = result.get();
		Iterator<Row<String, String, Integer>> rowsIterator = rows.iterator();
		
		// Populate our DocTermCount list from row data
		while (rowsIterator.hasNext()) {
			Row<String, String, Integer> row = rowsIterator.next();

			DocTermCount myDtc = new DocTermCount();
			myDtc.setDocUrl(row.getKey());

			// now get term counts from columns
			ColumnSlice<String, Integer> mySlice = row.getColumnSlice();
			List<HColumn<String, Integer>> myCols = mySlice.getColumns();
			Map<String, Integer> myTermCount = new HashMap<String, Integer>();
			for (HColumn<String, Integer> col : myCols) {
				myTermCount.put(col.getName(), col.getValue());
			}
			myDtc.setTermCounts(myTermCount);
			
			myDocTerms.add(myDtc);
		}
				
        logger.debug("Total DocTermcounts retrieved: " + myDocTerms.size());
        
		return myDocTerms;        
	}
}
