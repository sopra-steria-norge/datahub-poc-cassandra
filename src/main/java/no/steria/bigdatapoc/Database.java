package no.steria.bigdatapoc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class Database {
    public static final String keyspaceName = "POC1";
    private static final String tableName = keyspaceName + ".power";
    private Session session;

    private static class DatabaseHolder {
        private static final Database instance = new Database();
    }

    public static Database getInstance() {
        return DatabaseHolder.instance;
    }

    public void setUp() {
        Cluster cluster = new Cluster.Builder().addContactPoints("localhost").build();
        session = cluster.connect();

        Metadata metadata = cluster.getMetadata();
        System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));
        session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }");
        System.out.println("Keyspace " + keyspaceName + " created");
        session.execute("CREATE TABLE " + tableName + "(timestamp timestamp,stationid text, kw double, council text, PRIMARY KEY(timestamp, stationid))");
        System.out.println("Table " + tableName + " created");
    }

    public void tearDown() {

        String dropKeyspace = "DROP KEYSPACE " + keyspaceName;

        this.session.execute(dropKeyspace);
        System.out.println("Keyspace DROPPED");
    }

    public Session getSession() {
        return session;
    }
}
