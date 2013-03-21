package utexas.cid.news;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stores a relation between a document and its term counts
 *
 */
public class DocTermCount {

	private String docUrl = "";
	private Map<String, Integer> termCounts = new LinkedHashMap<String, Integer>();
	
	public String getDocUrl() {
		return docUrl;
	}
	public void setDocUrl(String url) {
		this.docUrl = url;
	}
	public Map<String, Integer> getTermCounts() {
		return termCounts;
	}
	public void setTermCounts(Map<String, Integer> termCounts) {
		this.termCounts = termCounts;
	}

}
