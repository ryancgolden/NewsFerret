/**
 * Copyright (c) 2013, Ryan Golden. 
 */
package utexas.cid.news.dataservices;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
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
import utexas.cid.news.NewsArticle;

/**
 * CRUD like services for identity news items
 * @author Ryan Golden
 *
 */
public class NewsDao {
	
	private static final Logger logger = LoggerFactory
			.getLogger(NewsDao.class);

	// Column names
	public static final String COL_URL = "url";
	public static final String COL_AUTHOR = "author";
	public static final String COL_PUB_DATE = "pubdate";
	public static final String COL_FEED_URI = "feeduri";
	public static final String COL_DESCRIPTION = "description";
	public static final String COL_TITLE = "title";
	public static final String COL_CONTENT = "content";
	public static final String COL_UPDATE_TIME = "updatetime";
	
	private static NewsDao sInstance = null;
	private static Keyspace sKeyspace = null;
	private static ColumnFamilyTemplate<String, String> sTemplate = null;
	
	/**
	 * Use static factory method to get instance
	 */
	private NewsDao() {
		Cluster myCluster = HFactory.getOrCreateCluster(Constants.CLUSTER, Constants.HOST_IP);
		KeyspaceDefinition keyspaceDef = null;
		if (myCluster != null) {
			keyspaceDef = myCluster.describeKeyspace(Constants.KEYSPACE);
		}

		// If keyspace does not exist, the CF's don't exist either. => create them
		if (keyspaceDef == null && myCluster != null) {
			NewsSchema.createNewsColumnFamily(myCluster);
		}
		
		sKeyspace = HFactory.createKeyspace(Constants.KEYSPACE, myCluster);
		sTemplate =	new ThriftColumnFamilyTemplate<String, String> (
				sKeyspace,
				Constants.NEWS_COLUMN_FAMILY, 
				StringSerializer.get(), 
				StringSerializer.get());
	}
	
	/**
	 * Public static factory method to get instance.  Use this to get a NewsDao
	 * instance
	 * @return NewsDao instance
	 */
	public static NewsDao getInstance() {
		if (sInstance != null) {
			return sInstance;
		}
		sInstance = new NewsDao();
		return sInstance;
	}
	
	/**
	 * Create or update a news story with the given URL, title, date, and content
	 * URL acts as primary key
	 * 
	 * All args are String.
	 * 
	 * @param pUrl - URL of news story
	 * @param pTitle - title of news story
	 * @param pAuthor - author of story
	 * @param FeedUri - uniform resource indicator of story
	 * @param pPubDate - date story was published
	 * @param pDescription - Feed's description of news story
	 * @param pContent - String content of news story
	 */
	public void update(NewsArticle pNews) {		

		// Validate data
		if (pNews == null || pNews.url == null || pNews.title == null ||
				pNews.pubDate == null || pNews.content == null) {
			throw new RuntimeException("No null values allowed for news update");
		}

		// <String, String> correspond to key and Column name.
		ColumnFamilyUpdater<String, String> updater = sTemplate.createUpdater(pNews.url);
		updater.setString(COL_URL, pNews.url);
		updater.setString(COL_TITLE, pNews.title);
		updater.setString(COL_AUTHOR, pNews.author);
		updater.setString(COL_FEED_URI, pNews.feedUri);
		updater.setString(COL_PUB_DATE, pNews.getPubDateAsString());
		updater.setString(COL_DESCRIPTION, pNews.description);
		updater.setString(COL_CONTENT, pNews.content);
		updater.setString(COL_UPDATE_TIME, Long.toString(System.currentTimeMillis()));
	
		try {
		    sTemplate.update(updater);
		} catch (HectorException e) {
		    System.err.println("Problem updating the following:");
		    System.err.println(COL_URL + ":" + pNews.url);
		    System.err.println(COL_TITLE + pNews.title);
		    System.err.println(COL_AUTHOR + pNews.author);
		    System.err.println(COL_FEED_URI + pNews.feedUri);
		    System.err.println(COL_PUB_DATE + pNews.getPubDateAsString());
		    System.err.println(COL_DESCRIPTION + pNews.description);
		    System.err.println(COL_CONTENT + pNews.content);
			e.printStackTrace();
		}
	}

	/**
	 * Return a NewsArticle for the given URL, if it exists in the store.
	 * 
	 * @param pUrl - URL of the news story
	 * @return populated NewsArticle else null
	 */
	public NewsArticle read (String pUrl) {
	
		NewsArticle myNews = new NewsArticle();
		try {
			ColumnFamilyResult<String, String> res = sTemplate.queryColumns(pUrl);
			myNews.setUrl(res.getString(COL_URL));
			myNews.setTitle(res.getString(COL_TITLE));
			myNews.setAuthor(res.getString(COL_AUTHOR));
			myNews.setFeedUri(res.getString(COL_FEED_URI));
			myNews.setPubDate(res.getString(COL_PUB_DATE));
			myNews.setDescription(res.getString(COL_DESCRIPTION));
			myNews.setContent(res.getString(COL_CONTENT));
			myNews.setUpdateTime(res.getString(COL_UPDATE_TIME));
			
		} catch (HectorException e) {
		    System.err.println("Problem reading the following:");
		    System.err.println(COL_URL + ":" + pUrl);
			e.printStackTrace();
			return null;
		}
		return myNews;
	}

	/**
	 * Delete row for given URL key.
	 * @param pUrl - String URL of row to delete
	 */
	void delete(String pUrl) {
		try {
			sTemplate.deleteRow(pUrl);
		} catch (HectorException e) {
		    System.err.println("Problem deleting the following:");
		    System.err.println(COL_URL + ":" + pUrl);
			e.printStackTrace();
		}
	}
	
		
	/**
	 * Print all news stories into a single Map of Maps.
	 * XXX: Memory hog, should probably accept a callback class instead.
	 */
	public void printAll() {
		logger.debug("Reading all from NewsDao");
		
        int row_limit = 200;
		int rowCount = 0;

        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
            .createRangeSlicesQuery(sKeyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
            .setColumnFamily(Constants.NEWS_COLUMN_FAMILY)
            .setRange(null, null, false, 10)
            .setRowCount(row_limit);

        String last_key = null;

        while (true) {
            rangeSlicesQuery.setKeys(last_key, null);
            logger.debug(" > " + last_key);

            QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<String, String, String> rows = result.get();
            Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

            // we'll skip this first one, since it is the same as the last one from previous time we executed
            if (last_key != null && rowsIterator != null) rowsIterator.next();   

            while (rowsIterator.hasNext()) {
            	rowCount++;
              Row<String, String, String> row = rowsIterator.next();
              last_key = row.getKey();

              if (row.getColumnSlice().getColumns().isEmpty()) {
                continue;
              }
              
              logger.debug("key: " + row.getKey());
              for (HColumn<String, String> col : row.getColumnSlice().getColumns()) {
            	  logger.debug("  {" + col.getName() + ":" +
            			  col.getValue() + ":" + new Date(col.getClock()) + "}");
              }
            }

            if (rows.getCount() < row_limit)
                break;
        }
        logger.info("Total rows retrieved: " + rowCount);
        
		
	}
	
	/**
	 * Insert, retrieve, and delete a test record
	 * @param args
	 */
	public static void main(String[] args) {
		NewsDao myDao = NewsDao.getInstance();
//		String myTestUrl = "http://testurl:909090/somestory"; 
//		myDao.update(myTestUrl,
//				"Identity Theft Occurs",
//				"Joe Reporter",
//				"uri:news.com/something/somewhere",
//				"2012-02-15 08:20:34",
//				"An identity theft occurred last night",
//				"Here is the actual store of what happened...");
//		logger.debug("Update complete.");
//		Map<String, String> myMap = myDao.read(myTestUrl);
//		for (String key : myMap.keySet()) {
//			logger.debug(key + ":" + myMap.get(key));
//		}
//		logger.debug("Read complete.");
//		myDao.delete(myTestUrl);
//		logger.debug("Delete complete.");
//		myMap = myDao.read(myTestUrl);
//		if (myMap.isEmpty()) {
//			logger.debug("Deletion verified.");
//		}
		myDao.printAll();
		
	}

	/**
	 * Read all metadata about all news stories in the store
	 * XXX: this is not scalable right now... just reading a big list into memory
	 * @return
	 */
	public List<NewsArticle> readAll() {
		logger.debug("Reading all from NewsDao");
		
		List<NewsArticle> myList = new ArrayList<NewsArticle>();
        int row_limit = 500;
		int rowCount = 0;

        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
            .createRangeSlicesQuery(sKeyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
            .setColumnFamily(Constants.NEWS_COLUMN_FAMILY)
            .setRange(null, null, false, 10)   // shouldn't need more than 10 cols
            .setRowCount(row_limit);

        String last_key = null;

        while (true) {
            rangeSlicesQuery.setKeys(last_key, null);
            logger.debug(" > " + last_key);

            QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<String, String, String> rows = result.get();
            Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

            // we'll skip this first one, since it is the same as the last one from previous time we executed
            if (last_key != null && rowsIterator != null) rowsIterator.next();   

            while (rowsIterator.hasNext()) {
            	rowCount++;
            	Row<String, String, String> row = rowsIterator.next();
            	last_key = row.getKey();

            	if (row.getColumnSlice().getColumns().isEmpty()) {
            		continue;
            	}
              
            	logger.debug("key: " + row.getKey());
            	NewsArticle myArticle = new NewsArticle();
            	for (HColumn<String, String> col : row.getColumnSlice().getColumns()) {
            		String name = col.getName();
            		String val = col.getValue();
            		logger.debug("  reading {" + col.getName() + ":" + col.getValue() + "}");
            		if (NewsDao.COL_TITLE.equals(name)) {
            			myArticle.setTitle(val);
            		} else if (NewsDao.COL_AUTHOR.equals(name)) {
            			myArticle.setAuthor(val);
            		} else if (NewsDao.COL_CONTENT.equals(name)) {
            			// myArticle.setContent(val);
            			// do nothing for now... we have a different keyspace for this info
            		} else if (NewsDao.COL_DESCRIPTION.equals(name)) {
            			myArticle.setDescription(val);
            		} else if (NewsDao.COL_FEED_URI.equals(name)) {
            			myArticle.setFeedUri(val);
            		} else if (NewsDao.COL_PUB_DATE.equals(name)) {
            			//myArticle.setPubDate(val);
            		} else if (NewsDao.COL_UPDATE_TIME.equals(name)) {
            			myArticle.setUpdateTime(val);
            		} else if (NewsDao.COL_URL.equals(name)) {
            			myArticle.setUrl(val);
            		}
            	}
            	myList.add(myArticle);
            }

            if (rows.getCount() < row_limit)
                break;
        }
        logger.info("Total rows retrieved: " + rowCount);
        return myList;
        
	}


}
