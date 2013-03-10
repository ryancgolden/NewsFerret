News Ferret
===========
README for Identity News Ferret

(c) 2013, Ryan Golden, Center for Identity, University of Texas at Austin


PRE-REQUISITES
--------------
* Apache Hadoop (tested using 1.0.4)
* Apache Cassandra (testing using 1.0.12)
* Java 6 JDK (tested using JDK from OpenJDK 1.6.0_27)
* Matlab and Simulink (tested using R2009a)
* Git (tested using 1.7.9.5)

Also recommended:
* Linux (tested on Ubuntu Linux 12.04, 3.2 kernel)
* Eclipse IDE

In theory, the software could also run on Windows, but I had difficulty configuring Hadoop and Cassandra.

Additionally, I don't recommend using Java 7 yet.  Hadoop does not work well with Java 7 as of this writing (Mar, 2013).

GETTING THE SOURCE
------------------
The source is available on GitHub.  See https://github.com/ryancgolden/NewsFerret

1. Install Git.
2. 'cd' to the directory where you want to put the code,
3. Run:
git clone https://github.com/ryancgolden/NewsFerret.git
4. This should create a folder called "NewsFerret" containing the source.  I'll refer to the path to the NewsFerret folder as $NEWSFERRET below.

The source is a mixture of Java and MATLAB code

* Java:
    NewsGrabber - scrapes news feeds for identity news articles
    NewsAnalyzer - creates term-document-matrices and summary stats
    Various other utilities
* MATLAB:
    document and term clustering
    top 25 words per document
    Latent Semantic Analysis (LSA) techniques
    pairwise term similarity

BUILDING
--------
1. Install Eclipse.
2. Open and create a new workspace folder (e.g., "~/workspace")
3. Import the following Existing Projects into your workspace:
    - $NEWSFERRET/src/NewsAnalyzer
    - $NEWSFERRET/src/NewsGrabber
4. Define the following classpath variables in your workspace preferences (Window->Preferences->Java->Build Path->Classpath Variables):
    - ANALYZER_LIB_ROOT = $NEWSFERRET/lib/NewsAnalyzer
    - GRABBER_LIB_ROOT = $NEWSFERRET/lib/NewsGrabber
    - LIBSRC_ROOT = $NEWSFERRET/libsrc
    - (optional) CASSANDRA_SRC_ROOT = [root of cassandra source]
    - (optional) THRIFT_SRC_ROOT = [root of thrift source]
5. Build the projects.  Usually Eclipse is set to auto-build by default.

RUNNING GRABBER and JAVA ANALYZERS
----------------------------------
1. Make sure the Cassandra and Hadoop are properly installed and Cassandra services are running.

2. Edit the utexas.cid.news.Constants class to give with the appropriate information for your Cassandra installation:
    - CLUSTER = "My Cluster" (will be created)
    - HOST = "localhost" (cassandra service must be available at this host)
    - PORT = "9160" (cassandra service must be available at this port)
    - HOST_IP = "localhost:9160" (lazy concatenation of the above fields)
    - REPLICATION_FACTOR = 1 (leave as 1 or read Cassandra documentation for more info)
    - KEYSPACE = "identity" (will be created)
    - NEWS_COLUMN_FAMILY = "News" (will be created for storing and querying article content and metadata)
    - DOC_TERM_COLUMN_FAMILY = "DocTerm" (will be created for storing querying term-doc matrix)

3. Create a class with a main method that calls utexas.cid.news.dataservices.NewsSchema.createEverything() method.    I don't offer an out-of-the box class to do this because I don't want folks accidentally overwriting their existing Cassandra keyspaces, until a safer schema creation technique is available.

4. If you want to use a different set of news feeds, append or remove feed URL Strings to feedUrls variable in utexas.cid.news.NewsGrabber.

5. Run utexas.cid.news.NewsGrabber
    - Queries all news feeds
    - Pulls news article from URL
    - Uses BoilerPipe to extract the main content of the article from the HTML
    - Inserts article metadata and content into the NEWS_COLUMN_FAMILY keyspace in Cassandra
    - Overwrites any existing articles with the same URL.
    - NOTE: Does not delete older articles, so generally it is safe to run this one or more times per day, which will incrementally accumulate articles in your database

6. Run utexas.cid.news.analysis.Analyzer
    - Pass as first argument the path to $NEWSFERRET/src/matlab/dat
    - Runs Hadoop map-reduce job to populate DOC_TERM_COLUMN_FAMILY with term-doc matrix
    - Outputs some summary data to log
    - Creates *.dat files below the dir you passed for import into MATLAB
    optionally, you can generate a jar using the jardesc and run $NEWSFERRET/bin/runAnalyzer.sh

RUNNING MATLAB ANALYSIS
-----------------------
[TODO: more info here]

1. Ensure the *.dat files from above are at $NEWSFERRET/src/matlab/dat folder, if they aren't there already
2. Run $NEWSFERRET/src/matlab/main_id.m to perform clustering and get top 25 words per doc
3. Run $NEWSFERRET/src/matlab/termsim.m to get pairwise similarity cell array
4. XXX: Open "c" in variable editor, copy and paste to Excel, to get pairwise similarity information into a spreadsheet.  As of this writing, I have not had good luck with using 'xlswrite' or 'csvwrite' functions with cell arrays.
5. Explore the MATLAB code and variables!

QUESTIONS/COMMENTS
------------------
Contact Ryan Golden at his gmail alias: ryan c golden (no spaces)
Or see additional contact information at http://identity.utexas.edu


