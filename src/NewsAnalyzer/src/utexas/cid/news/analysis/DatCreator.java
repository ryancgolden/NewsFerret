package utexas.cid.news.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utexas.cid.news.DocTermCount;
import utexas.cid.news.NewsArticle;
import utexas.cid.news.dataservices.DocTermDao;
import utexas.cid.news.dataservices.NewsDao;

/**
 * Methods for:
 * 1) Preparing data for our MATLAB functions
 * 2) Exporting prettier output, after our MATLAB functions have done their work
 * 
 * @author Ryan Golden -- ryancgolden@gmail.com
 */
public class DatCreator {

	/** The logger. */
	private static Logger logger = LoggerFactory.getLogger(DatCreator.class);

	// this will be appended to the passed-in root directory to
	// contain the output files
	public static String sOutDir = "termdoc";

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//prepareForMatlab(args[0]);
		createPrettyPairsDat(args[0]);
		//createNewsDatFiles(args[0] + File.separator + "articles");
	}

	/**
	 * Write vector space model (e.g., term-doc matrix) files for 
	 * consumption by our MATLAB functions.
	 * 
	 * XXX: Not scalable. Should page through Articles and doc terms
	 * rather than read them all into memory.
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
	 *            - String absolute dir below which output will be written -
	 *            NOTE: importantwords.txt must also exist here, in order to -
	 *            to create importantstems.dat
	 * @throws Exception
	 */
	public static void prepareForMatlab(String pRootDatDir) throws Exception {

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
		String myImportantWordsTxtFile = pRootDatDir + sep
				+ "importantwords.txt";
		String myImportantStemsDatFile = pRootDatDir + sep
				+ "importantstems.dat";
		String myImportantStemsMapDatFile = pRootDatDir + sep
				+ "importantstemsmap.dat";
		String myStopWordsTxtFile = pRootDatDir + sep + "stopwords.txt";
		String myStopStemsDatFile = pRootDatDir + sep + "stopstems.dat";
		String myStopStemsMapDatFile = pRootDatDir + sep + "stopstemsmap.dat";

		// Convert importantwords.txt into importantstems.dat
		// which has the same terms, but stemmed. This allows importantwords.txt
		// to be maintained as a simple English text file
		createStemsDat(myImportantWordsTxtFile, myImportantStemsDatFile,
				myImportantStemsMapDatFile);

		// Since we convert importantwords, we also need to convert
		// stopwords.txt
		createStemsDat(myStopWordsTxtFile, myStopStemsDatFile,
				myStopStemsMapDatFile);

		DocTermDao myDocTermDao = DocTermDao.getInstance();

		// Map of terms to an array of int, each int representing how many
		// times the term appears for the corresponding doc index
		Map<String, Integer[]> myTermDocMat = new HashMap<String, Integer[]>();

		List<String> myDocList = new ArrayList<String>();
		List<String> myDocIdList = new ArrayList<String>();

		// First get a list of all docs
		NewsDao myNewsDao = NewsDao.getInstance();
		List<NewsArticle> myDocs = myNewsDao.readAll();

		// Now create a map to index those docs by id (url)
		Map<String, NewsArticle> myDocMap = new HashMap<String, NewsArticle>();
		for (NewsArticle doc : myDocs) {
			logger.debug("Preparing VSM for " + doc.title);
			myDocMap.put(doc.getUrl(), doc);
		}

		// print a column in the matrix for each DocTerm row
		int i = 0;

		List<DocTermCount> myDocTerms = myDocTermDao.readAll();

		for (DocTermCount d : myDocTerms) {
			Map<String, Integer> myTermCount = d.getTermCounts();
			myDocList.add(d.getDocUrl());
			myDocIdList.add(d.getDocUrl());

			// Now add it to the master matrix
			for (String word : myTermCount.keySet()) {
				if (myTermDocMat.containsKey(word)) {
					Integer[] counts = myTermDocMat.get(word);
					counts[i] = myTermCount.get(word);
				} else {
					// initialize the row with zeros, then set the count for
					// this doc
					Integer[] counts = new Integer[myDocTerms.size()];
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
		List<String> myWordList = Arrays.asList(myTermDocMat.keySet().toArray(
				new String[0]));
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

		logger.info("Done writing data files.");
	}

	/**
	 * Create stems from important words
	 * 
	 * @param pWordsTxtFile
	 *            - location of input txt file
	 * @param pStemsDatFile
	 *            - output location of stems file
	 * @param pStemsMapDatFile
	 *            - output location of stem -> [word] map file
	 * @throws FileNotFoundException
	 *             if input txt not found
	 * @throws IOException
	 *             - if output file(s) can't be written
	 */
	private static void createStemsDat(String pWordsTxtFile,
			String pStemsDatFile, String pStemsMapDatFile)
			throws FileNotFoundException, IOException {

		Scanner sc = null;
		BufferedWriter out = null;
		BufferedWriter outMap = null;
		Map<String, Set<String>> stemMap = new HashMap<String, Set<String>>(); // preserve
																				// order

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
			throw (e);
		} catch (IOException ioe) {
			logger.error("Problem creating stemming output", ioe);
			throw (ioe);
		} finally {
			if (sc != null) {
				sc.close();
			}
			if (out != null) {
				out.close();
			}
			if (outMap != null) {
				outMap.close();
			}
		}
	}

	/**
	 * Utility method to sort a Collection
	 * 
	 * @param c
	 *            - Collection to sort
	 * @return sorted List
	 */
	public static <T extends Comparable<? super T>> List<T> asSortedList(
			Collection<T> c) {
		List<T> list = new ArrayList<T>(c);
		java.util.Collections.sort(list);
		return list;
	}

	/**
	 * Makes the stemmed pairs output prettier by substituting back the original
	 * words
	 * 
	 * @param pRootDatDir
	 *            - root to dat dir in which the following files will be used:
	 *            <rootDatDir>/news/importantpairs.dat
	 *            <rootDatDir>/news/importantpairspretty.dat
	 *            <rootDatDir>/importantstemsmap.dat
	 * 
	 * @throws FileNotFoundException
	 *             if inputs cannot be found
	 * @throws IOException
	 *             - if output file(s) can't be written
	 */
	public static void createPrettyPairsDat(String pRootDatDir)
			throws FileNotFoundException, IOException {

		String sep = File.separator;
		String myStemPairsDatFile = pRootDatDir + sep + "importantpairs.csv";
		String myPrettyPairsDatFile = pRootDatDir + sep
				+ "importantpairspretty.csv";
		String myRelatedDocsFile = pRootDatDir + sep + "importantpairs_topmatchingdocs.csv";
		String myPrettyRelatedDocsFile = pRootDatDir + sep
				+ "importantpairs_topmatchingdocs_pretty.csv";
		String myStemsMapDatFile = pRootDatDir + sep + "importantstemsmap.dat";

		Scanner scMap = null;
		Scanner sc = null;
		BufferedWriter out = null;
		BufferedWriter outRelated = null;
		Map<String, Set<String>> stemMap = new LinkedHashMap<String, Set<String>>();

		try {
			// Read in Stem Map from dat file
			scMap = new Scanner(new File(myStemsMapDatFile));
			while (scMap.hasNextLine()) {
				String myLine = scMap.nextLine();
				String[] myStrs = myLine.split("\\s+");
				String myStem = "";
				Set<String> myWordSet = new LinkedHashSet<String>();
				for (int i = 0; i < myStrs.length; i++) {
					if (i == 0) {
						myStem = myStrs[i];
						continue;
					} else {
						myWordSet.add(myStrs[i]);
					}
				}
				stemMap.put(myStem, myWordSet);
			}

			// Read in stem pair weights from dat file, replacing in
			// out file with the expanded word set, making it more readable
			sc = new Scanner(new File(myStemPairsDatFile));
			out = new BufferedWriter(new FileWriter(myPrettyPairsDatFile));

			// hack to skip first line with headers
			out.write(sc.nextLine() + "\n");
			
			while (sc.hasNextLine()) {
				String myLine = sc.nextLine();
				String[] myStrs = myLine.split("\\s+");
				if (myStrs.length != 4) {
					logger.warn("stem pair weights input error, line doesn't have 4 items.");
					continue;
				}
				String term1 = myStrs[0];
				String term2 = myStrs[1];
				String weight = myStrs[2];
				String type = myStrs[3];

				StringBuffer words1 = new StringBuffer();
				StringBuffer words2 = new StringBuffer();
				for (String word : stemMap.get(term1)) {
					words1.append(word + " ");
					// hack: only use the first word to avoid ugly multi-word cells
					break;
				}
				String words1Str = words1.substring(0, words1.lastIndexOf(" "));
				for (String word : stemMap.get(term2)) {
					words2.append(word + " ");
					// hack: only use the first word to avoid ugly multi-word cells
					break;
				}
				String words2Str = words2.substring(0, words2.lastIndexOf(" "));
				out.write("\"" + words1Str + "\" \"" + words2Str + "\" "
						+ weight + " \"" + type + "\"");
				out.newLine();
			}

			logger.info("Done writing " + myPrettyPairsDatFile);
			
			// Read in stem pair related docs from csv file, replacing stems in
			// out file with the expanded words, making it more readable
			sc = new Scanner(new File(myRelatedDocsFile));
			outRelated = new BufferedWriter(new FileWriter(myPrettyRelatedDocsFile));

			// hack to skip first line with headers
			outRelated.write(sc.nextLine() + "\n");
			
			while (sc.hasNextLine()) {
				String myLine = sc.nextLine();
				String[] myStrs = myLine.split("\",\"");
				if (myStrs.length != 9) {
					logger.warn("related docs input error, line doesn't have 9 items.");
					outRelated.write(myLine);
					outRelated.newLine();
					continue;
				}
				String term1 = myStrs[0].replaceAll("\\\"", "");
				String term2 = myStrs[1].replaceAll("\\\"", "");;
				String weight = myStrs[2];
				String type = myStrs[3].replaceAll("\\\"", "");
				String doc1 = myStrs[4].replaceAll("\\\"", "");
				String doc2 = myStrs[5].replaceAll("\\\"", "");
				String doc3 = myStrs[6].replaceAll("\\\"", "");
				String doc4 = myStrs[7].replaceAll("\\\"", "");
				String doc5 = myStrs[8].replaceAll("\\\"", "");

				StringBuffer words1 = new StringBuffer();
				StringBuffer words2 = new StringBuffer();
				for (String word : stemMap.get(term1)) {
					words1.append(word + " ");
					// hack: only use the first word to avoid ugly multi-word cells
					break;
				}
				String words1Str = words1.substring(0, words1.lastIndexOf(" "));
				for (String word : stemMap.get(term2)) {
					words2.append(word + " ");
					// hack: only use the first word to avoid ugly multi-word cells
					break;
				}
				String words2Str = words2.substring(0, words2.lastIndexOf(" "));
				outRelated.write("\"" + words1Str + "\", \"" + words2Str + "\", " + weight + ", \"undirected\", \""
						+ doc1 + "\", \"" + doc2 + "\", \"" + doc3 + "\", \"" + doc4 + "\", \"" + doc5 + "\"");
				outRelated.newLine();
			}

			logger.info("Done writing " + myPrettyRelatedDocsFile);
			
		} catch (FileNotFoundException e) {
			logger.error("Problem reading " + myStemPairsDatFile + " or "
					+ myStemsMapDatFile, e);
			throw (e);
		} catch (IOException ioe) {
			logger.error("Problem creating pretty output", ioe);
			throw (ioe);
		} finally {
			if (scMap != null) {
				scMap.close();
			}
			if (sc != null) {
				sc.close();
			}
			if (out != null) {
				out.close();
			}
			if (outRelated != null) {
				outRelated.close();
			}
		}
	}

	/**
	 * Not needed by our main routine, but useful if other tools want access to
	 * the raw content of the news articles.
	 * @param pRootDatDir directory to which to output the files.
	 */
	public static void createNewsDatFiles(String pRootDatDir) throws Exception {
		
		try {
			File root = new File(pRootDatDir);
			if (!root.canWrite()) {
				throw new Exception("Can't write to given directory: " + pRootDatDir);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
			
		NewsDao myNewsDao = NewsDao.getInstance();
		List<NewsArticle> myArticles = myNewsDao.readAll();
		int count = 0;
		for (NewsArticle n : myArticles) {
			count++;
			BufferedWriter out = null;
			String fName = "";
			try {
				String fNumber = String.format("%04d",  count);
				String fTitle = n.title.replaceAll("\\W", "");
				fName = fNumber + "-" + fTitle + ".txt";
				out = new BufferedWriter(new FileWriter(pRootDatDir + File.separator + fName));
				out.write(n.content);
			} catch (Exception e) {
				logger.warn("Problem writing article data file: " + fName, e);
			} finally {
				if (out != null) {
					out.close();
				}
			}
			
		}
		
	}

	
}
