/**
 * 
 */
package utexas.cid.news;

import java.util.Date;

/**
 * Represents a single news article from a news feed
 * @author rgolden
 */
public class NewsArticle {

	public String url;
	public String author;
	public Date pubDate;
	public String feedUri;
	public String description;
	public String title;
	public String content;
	public Date updateTime;
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public Date getPubDate() {
		return pubDate;
	}
	/**
	 * Return String of millis since epoch.
	 */
	public String getPubDateAsString() {
		return Long.toString(pubDate.getTime());
	}
	public void setPubDate(Date pubDate) {
		this.pubDate = pubDate;
	}
	/**
	 * Set date using String version of long in millis
	 * @param pubDate - String of long value representing millis since epoch
	 */
	public void setPubDate(String pubDate) {
		this.pubDate = new Date(Long.valueOf(pubDate));
	}
	public String getFeedUri() {
		return feedUri;
	}
	public void setFeedUri(String feedUri) {
		this.feedUri = feedUri;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public Date getUpdateTime() {
		return updateTime;
	}
	/**
	 * Return String of millis since epoch.
	 */
	public String getUpdateTimeAsString() {
		return Long.toString(updateTime.getTime());
	}
	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	/**
	 * Set date using String version of long in millis
	 * @param pubDate - String of long value representing millis since epoch
	 */
	public void setUpdateTime(String updateTime) {
		this.updateTime = new Date(Long.valueOf(updateTime));
	}

}
