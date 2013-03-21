package utexas.cid.news;

import java.net.MalformedURLException;
import java.net.URL;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

/**
 * Services to fetch and scrub the primary article content from an HTML news,
 * i.e., remove boilerplate HTML, ads, etc. article
 * 
 * @author Ryan Golden
 * 
 */
public class Scrubber {

	/**
	 * Scrub a test URL
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		String myUrlString = "http://www.sun-sentinel.com/news/opinion/editorials/fl-editorials-identity-theft-20130218,0,4333441.story";
		try {
			String text = Scrubber.getContent(myUrlString);
			System.out.println("Content:");
			System.out.println(text);
		} catch (MalformedURLException e) {
			System.err.println("Problem with the URL: " + myUrlString);
			e.printStackTrace();
		}
	}

	/**
	 * Returns the main content (e.g., news article text) for a given URL. If
	 * there is an error, an exception is printed to stderr and empty string is
	 * returned.
	 * 
	 * @param pUrlString
	 *            - string containing URL with content to extract
	 * @return String of main content (non HTML)
	 * @throws MalformedURLException
	 */
	public static String getContent(String pUrlString)
			throws MalformedURLException {
		URL myUrl = new URL(pUrlString);
		try {
			return ArticleExtractor.INSTANCE.getText(myUrl);
		} catch (BoilerpipeProcessingException e) {
			System.err.println("Problem getting and cleaning content for url: "
					+ pUrlString);
			e.printStackTrace();
		}
		return "";
	}

}
