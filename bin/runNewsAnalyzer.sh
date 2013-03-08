#!/bin/sh
# Execute the News Analyzer Cassandra-based Hadoop Map Reduce job
# Requires:
# 	Cassandra 1.0.12
#	Hadoop 1.0.4
# 	Linux

cwd=`dirname $0`

# NewsAnalyzer Jar
if [ ! -e $cwd/NewsAnalyzer.jar ]; then
    echo "Unable to locate ./NewsAnalyzer jar" >&2
    exit 1
fi
# Required libs
if [ ! -e $cwd/../lib/NewsAnalyzer ]; then
    echo "Unable to locate ../lib directory" >&2
    exit 1
fi

CLASSPATH=$CLASSPATH:$cwd/NewsAnalyzer.jar
for jar in $cwd/../lib/NewsAnalyzer/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

if [ -x $JAVA_HOME/bin/java ]; then
    JAVA=$JAVA_HOME/bin/java
else
    JAVA=`which java`
fi

if [ "x$JAVA" = "x" ]; then
    echo "Java executable not found (hint: set JAVA_HOME)" >&2
    exit 1
fi

#OUTPUT_REDUCER=cassandra

#echo $CLASSPATH
echo Command:
echo $JAVA -Xmx1G -ea -cp $CLASSPATH utexas.cid.news.analysis.Analyzer
#$JAVA -Xmx1G -ea -cp $CLASSPATH utexas.cid.news.analysis.WordCount output_reducer=$OUTPUT_REDUCER
$JAVA -Xmx1G -ea -cp $CLASSPATH utexas.cid.news.analysis.Analyzer
