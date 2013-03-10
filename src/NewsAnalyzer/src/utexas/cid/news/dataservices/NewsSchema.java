package utexas.cid.news.dataservices;

import java.util.Arrays;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utexas.cid.news.Constants;

/**
 * Cassandra schema creation helper class.
 * 
 * @author rgolden
 *
 */
public class NewsSchema {

	private static final Logger logger = LoggerFactory
			.getLogger(NewsSchema.class);

	public static void createNewsColumnFamily(Cluster pCluster) {
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(Constants.KEYSPACE,
                Constants.NEWS_COLUMN_FAMILY,
                ComparatorType.UTF8TYPE);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(Constants.KEYSPACE,
              ThriftKsDef.DEF_STRATEGY_CLASS,
              Constants.REPLICATION_FACTOR,
              Arrays.asList(cfDef));

		//Add the schema to the cluster.
		//"true" as the second param means that Hector will block until all nodes see the change.
		if (pCluster != null) {
			pCluster.addKeyspace(newKeyspace, true);
		} else {
			logger.error("Could not find cluster to which to add keyspace: " 
					+ Constants.KEYSPACE + " and col family: " + Constants.NEWS_COLUMN_FAMILY);
		}
	}

	/**
	 * Convenience method to create the col family without a cluster
	 */
	public static void createDocTermColumnFamily() {
		Cluster myCluster = HFactory.getOrCreateCluster(Constants.CLUSTER,
				Constants.HOST_IP);
		createDocTermColumnFamily(myCluster);
	}
	
	/**
	 * Create doc-term column family
	 * @param pCluster - using given cluster
	 */
	public static void createDocTermColumnFamily(Cluster pCluster) {
		logger.debug("Creating column family: " + Constants.DOC_TERM_COLUMN_FAMILY);
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(Constants.KEYSPACE,
                Constants.DOC_TERM_COLUMN_FAMILY,
                ComparatorType.UTF8TYPE);

		//Add the column family to the cluster.
		//"true" as the second param means that Hector will block until all nodes see the change.
		if (pCluster != null) {
			pCluster.addColumnFamily(cfDef, true);
		} else {
			logger.error("Could not find cluster to which to add keyspace: " 
					+ Constants.KEYSPACE + " and col family: " + Constants.NEWS_COLUMN_FAMILY);
		}		
		logger.debug("Creation successful.");

	}
	
	/**
	 * Convenience method to call drop without cluster.
	 */
	public static void dropDocTermColumnFamily() {
		Cluster myCluster = HFactory.getOrCreateCluster(Constants.CLUSTER,
				Constants.HOST_IP);
		dropDocTermColumnFamily(myCluster);

	}
	
	/**
	 * Drop term-doc column family from the keyspace
	 * @param pCluster - Cluster from which to drop the col family
	 */
	public static void dropDocTermColumnFamily(Cluster pCluster) {
		logger.debug("Dropping column family: " + Constants.DOC_TERM_COLUMN_FAMILY);

		if (pCluster != null) {
			try {
				// "true" as the 3rd param means that Hector will block
				// until all nodes see the change.
				pCluster.dropColumnFamily(Constants.KEYSPACE,
						Constants.DOC_TERM_COLUMN_FAMILY, true);
			} catch (Exception e) {
				logger.warn("Couldn't drop " + Constants.DOC_TERM_COLUMN_FAMILY
						+ ". If this is the first run, you can ignore this warning.", e);
			}
		} else {
			logger.error("Could not find cluster.");
		}		
		logger.debug("Drop successful.");

	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		addOutputCF();
	}
	
	/**
	 * Creates from scratch.  Be careful not to overwrite your data!
	 */
	public static void createEverything() {
		Cluster myCluster = HFactory.getOrCreateCluster(Constants.CLUSTER, Constants.HOST_IP);
		KeyspaceDefinition keyspaceDef = null;
		if (myCluster != null) {
			keyspaceDef = myCluster.describeKeyspace(Constants.KEYSPACE);
		} else {
			System.err.println("Could not connect to cluster " 
					+ Constants.CLUSTER + " at " + Constants.HOST_IP 
					+ ", please check Constants.java for correct cluster host and IP info.");
		}
		// If keyspace does not exist, the CF's don't exist either. => create them
		if (keyspaceDef == null && myCluster != null) {
			NewsSchema.createNewsColumnFamily(myCluster);  //also creates keyspace
			NewsSchema.createDocTermColumnFamily(myCluster);
		} else {
			System.out.println("Keyspace '" + Constants.KEYSPACE + "' already exists. Exiting");
		}
	}
	
	/**
	 * Creates just the output column family.
	 */
	public static void addOutputCF () {
		Cluster myCluster = HFactory.getOrCreateCluster(Constants.CLUSTER, Constants.HOST_IP);
		KeyspaceDefinition keyspaceDef = null;
		if (myCluster != null) {
			keyspaceDef = myCluster.describeKeyspace(Constants.KEYSPACE);
		} else {
			System.err.println("Could not connect to cluster " 
					+ Constants.CLUSTER + " at " + Constants.HOST_IP 
					+ ", please check Constants.java for correct cluster host and IP info.");
		}
		if (keyspaceDef != null && myCluster != null) {
			NewsSchema.createDocTermColumnFamily(myCluster);
			System.out.println("Successfully created output column family.");
		}		
	}

}
