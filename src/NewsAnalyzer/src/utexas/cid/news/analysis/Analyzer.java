/**
 * 
 */
package utexas.cid.news.analysis;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Primary analyzer driver class that creates analytical structures from the
 * identity news in our data store.  Starts the chained map reduce jobs and creates
 * output files for use by MATLAB code and other tools.
 * 
 * @author rgolden
 */
public class Analyzer {

    /** The logger. */
    private static Logger log = LoggerFactory.getLogger(Analyzer.class);
	
	/**
	 * 1) Run map-reduce job to create cassandra keyspace suitable for creating a term-doc matrix
	 * 2) Print stats to log (for information purposes only)
	 * 3) Create *.dat files for use by MATLAB
	 * 
	 * @param args - not used currently
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		String myDataOutputRootDir = args[0]; 
		
		log.info("Started Analyzer");
		// Map-reduce jobs run first
		// Let ToolRunner handle generic command-line options
		//ToolRunner.run(new Configuration(), new TermCounter(), args);
		ToolRunner.run(new Configuration(), new TermDocMatCreator(), args);

		// Print informational stats
		StatPrinter.printDocTermSummary();

		// Output DAT matrix file(s)
		DatCreator.createDatFiles(myDataOutputRootDir);

		log.info("Analyzer done");
		System.exit(0);
	}
}
