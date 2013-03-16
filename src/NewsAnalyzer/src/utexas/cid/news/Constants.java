package utexas.cid.news;

public class Constants {

	public static final String CLUSTER = "Ryan Cluster";
	public static final String HOST = "localhost";
	public static final String PORT = "9160";
	public static final String HOST_IP = "localhost:9160";
	public static int REPLICATION_FACTOR = 1;
	public static final String KEYSPACE = "identity";
	public static final String NEWS_COLUMN_FAMILY = "News";
	public static final String DOC_TERM_COLUMN_FAMILY = "DocTerm";

	// NOTE: Aside from setting this command-line, you also
	// must be sure the main_cli is in your MATLAB search path.  Do this
	// by setting MATLABPATH environmental variable when running the script,
	// e.g., in bash: 
	// 		export MATLABPATH=/newsferret/src/matlab
	public static final String MATLAB_CMD = "matlab -glnx86 -nosplash -nodesktop -logfile matlab.log -r main_cli";

}
