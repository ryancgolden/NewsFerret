package utexas.cid.news.analysis;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.tagger.maxent.TaggerConfig;

/**
 * Uses the Stanford NLP Group Part of Speech (POS) Tagger
 * http://nlp.stanford.edu/software/tagger.shtml
 * 
 * @author rgolden
 */
public class Tagger {

    /** The logger. */
    private static Log log = LogFactory.getLog(Tagger.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			MaxentTagger tagger = new MaxentTagger("taggers/english-bidirectional-distsim.tagger");
			
			String sample = "I gave my credit card and social security number to the clerk, Ryan Golden. The clerk took my social security number";
			String tagged = tagger.tagString(sample);
			
			log.info("Result: " + tagged);
			
			
		} catch (IOException e) {
			log.error("Problem loading tagger training files", e);
		} catch (ClassNotFoundException e) {
			log.error("Problem creating tagger", e);
		}
		
	}

}
