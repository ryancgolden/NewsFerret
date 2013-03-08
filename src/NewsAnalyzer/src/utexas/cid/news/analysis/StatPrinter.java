package utexas.cid.news.analysis;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utexas.cid.news.Constants;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import au.com.bytecode.opencsv.CSVReader;

public class StatPrinter {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String myPath = "/tmp/news/termcount/part-r-00000";

		// This only works if you run TermCounter map-reduce operation
		try {
			File f = new File(myPath);
			StatPrinter.printTermCountStats(f);
		} catch (Exception e) {
			System.err.println("Could not open " + myPath);
			// do nothing
		}

		printTermDocSummary();

	}

	private static StringSerializer stringSerializer = StringSerializer.get();

	/**
	 * XXX: this isn't scalable and cuts off at 2000 cols right now
	 * Though in a week of news scraping, I never found an article with
	 * more than 800 unique terms.
	 */
	private static void printTermDocSummary() {
		int maxCols = 0;
		Cluster cluster = HFactory.getOrCreateCluster(Constants.CLUSTER,
				Constants.HOST_IP);

		Keyspace keyspaceOperator = HFactory.createKeyspace(Constants.KEYSPACE,
				cluster);

		try {

			RangeSlicesQuery<String, String, Integer> rangeSlicesQuery = HFactory
					.createRangeSlicesQuery(keyspaceOperator, stringSerializer,
							stringSerializer, IntegerSerializer.get());
			rangeSlicesQuery.setColumnFamily(Constants.DOC_TERM_COLUMN_FAMILY);
			rangeSlicesQuery.setKeys("", "");
			rangeSlicesQuery.setRange("", "", false, 2000);
			rangeSlicesQuery.setRowCount(500);
			
			QueryResult<OrderedRows<String, String, Integer>> result = rangeSlicesQuery
					.execute();
			OrderedRows<String, String, Integer> orderedRows = result.get();

			Row<String, String, Integer> lastRow = orderedRows.peekLast();

			System.out.println("Contents of rows: \n");
			for (Row<String, String, Integer> r : orderedRows) {
				ColumnSlice<String, Integer> mySlice = r.getColumnSlice();
				List<HColumn<String, Integer>> myCols = mySlice.getColumns();
				System.out.println("   " + r);
				System.out.println("   # cols: " + myCols.size());
				if (myCols.size() > maxCols) {
					maxCols = myCols.size();
				}
			}
			System.out.println("Total rows: " + orderedRows.getCount());
			System.out.println("Max columns: " + maxCols);
			// System.out.println("Should have 11 rows: " +
			// orderedRows.getCount());

//			rangeSlicesQuery.setKeys(lastRow.getKey(), "");
//			orderedRows = rangeSlicesQuery.execute().get();
//
//			System.out.println("2nd page Contents of rows: \n");
//			for (Row<String, String, String> row : orderedRows) {
//				System.out.println("   " + row);
//			}

		} catch (HectorException he) {
			he.printStackTrace();
		}
		cluster.getConnectionManager().shutdown();

	}

	public static void printTermCountStats(File f) throws IOException {

		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(f), '\t');
			String[] nextLine;
			Map<Integer, List<String>> countMap = new HashMap<Integer, List<String>>();
			while ((nextLine = reader.readNext()) != null) {
				// nextLine[] is an array of values from the line
				if (nextLine[0] != null && nextLine[1] != null) {
					if (countMap.containsKey(Integer.valueOf(nextLine[1]))) {
						countMap.get(Integer.valueOf(nextLine[1])).add(
								nextLine[0]);
					} else {
						List<String> l = new ArrayList<String>();
						l.add(nextLine[0]);
						countMap.put(Integer.valueOf(nextLine[1]), l);
					}
				}
			}

			// Print sorted by number of occurrences
			List<Integer> keys = new ArrayList<Integer>();
			keys.addAll(countMap.keySet());
			Collections.sort(keys);
			for (Integer key : keys) {
				for (String value : countMap.get(key)) {
					System.out.println("" + key + " " + value);
				}
			}
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

}
