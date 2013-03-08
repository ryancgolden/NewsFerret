/**
 * 
 */
package utexas.cid.news.analysis;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Primary analyzer driver class that creates analytical structures from the
 * identity news in our data store.  Starts the chained map reduce jobs and creates
 * output files for use by MATLAB code and other tools.
 * 
 * @author rgolden
 */
public class Analyzer {

	/**
	 * 1) Run Term-Count Map Reduce Job
	 * 2) Doc-Term-Matrix MR job
	 * 3) Print stats to log (for information purposes only)
	 * 3) Create *.dat files for use by MATLAB
	 * 
	 * @param args - not used currently
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// Map-reduce jobs run first
		// Let ToolRunner handle generic command-line options
		//ToolRunner.run(new Configuration(), new TermCounter(), args);
		ToolRunner.run(new Configuration(), new TermDocMatCreator(), args);

		// Print informational stats
		//StatPrinter.printStats(new File("/tmp/news/termcount/part-r-00000"));

		// Output DAT matrix file(s)
		DatCreator.main(args);

		System.exit(0);
	}
}
