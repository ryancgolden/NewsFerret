/**
 * Copyright (c) 2013, Ryan Golden. 
 */
package utexas.cid.news;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utexas.cid.news.dataservices.NewsDao;

import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/**
 * Grabs news stories from a list of URLs and stores information for data mining
 * 
 * @author Ryan Golden
 *
 */
public class NewsGrabber {

	private static final Logger logger = LoggerFactory
			.getLogger(NewsGrabber.class);
	
	private static List<String> feedUrls = new ArrayList<String>();
	static {
		feedUrls.add("http://news.google.com/news?um=1&ned=us&hl=en&q=identity+theft&output=rss");
		feedUrls.add("http://www.huffingtonpost.com/tag/identity-theft/feed");
	}
	
	/**
	 * Grab news stories from all the feed URLs
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		NewsGrabber grabber = new NewsGrabber();
		for (String url : feedUrls) {
			grabber.grab(url);
		}
	}

	/**
	 * Grab info from the given feed URL and store it for data processing
	 * 
	 * @param feedUrl - String RSS URL of desired news feed
	 */
	private void grab(final String feedUrl) {
		SyndFeedInput input = new SyndFeedInput();
		SyndFeed feed = null;
		URL url = null;
		XmlReader reader = null;
		try {
			url = new URL(feedUrl);
			reader = new XmlReader(url);
			feed = input.build(reader);
			
			NewsDao myDao = NewsDao.getInstance();

			logger.debug("----------------FEED---------------------");
			logger.debug("Title          : " + feed.getTitle());
			logger.debug("Description    : " + feed.getDescription());
			logger.debug("Copyright      : " + feed.getCopyright());
			logger.debug("Feed Type      : " + feed.getFeedType());
			logger.debug("Language       : " + feed.getLanguage());
			logger.debug("Link           : " + feed.getLink());
			logger.debug("Published Date : " + feed.getPublishedDate());
//			logger.debug("URI            : " + feed.getUri());
//			logger.debug("Author         : " + feed.getAuthor());
//			logger.debug("Encoding       : " + feed.getEncoding());
//			logger.debug("Authors        : " + feed.getAuthors());
//			logger.debug("Categories     : " + feed.getCategories());
//			logger.debug("Contributors   : " + feed.getContributors());
//			logger.debug("Description Ex : " + feed.getDescriptionEx());
//			logger.debug("Title Ex       : " + feed.getTitleEx());
			
			String myContent = "";
			
			for (Iterator i = feed.getEntries().iterator(); i.hasNext();) {
		        SyndEntry entry = (SyndEntry) i.next();
				logger.debug("Updating title: " + entry.getTitle());
//				logger.debug("Title          : " + entry.getTitle());
//		        logger.debug("Author         : " + entry.getAuthor());
//		        logger.debug("Link           : " + entry.getLink());
//		        logger.debug("URI            : " + entry.getUri());
//		        logger.debug("Authors        : " + entry.getAuthors());
//		        logger.debug("Categories     : " + entry.getCategories());
//		        logger.debug("Description    : " + entry.getDescription());
//		        logger.debug("Published Date : " + entry.getPublishedDate());
		        
		        myContent = Scrubber.getContent(entry.getLink());
		        
		        String myDesc = "No Description";
		        SyndContent myDescSd = entry.getDescription();
		        if (myDescSd != null) {
		        	myDesc = myDescSd.getValue(); 
		        }
		        
		        NewsArticle myNews = new NewsArticle();
		        myNews.setUrl(entry.getLink());
		        myNews.setTitle(entry.getTitle());
		        myNews.setAuthor(entry.getAuthor());
		        myNews.setFeedUri(entry.getUri());
		        myNews.setPubDate(entry.getPublishedDate());
		        myNews.setDescription(myDesc);
		        myNews.setContent(myContent);
		        
		        myDao.update(myNews);

		    }
		} catch (IOException ioe) {
			logger.error("Problem reading URL: " + feedUrl);
			ioe.printStackTrace();
		} catch (FeedException fe) {
			logger.error("Problem with feed from URL: " + feedUrl);
			fe.printStackTrace();
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (Exception e) {
					logger.error("Problem closing reader");
					e.printStackTrace();
				}
	    }
	}

}
