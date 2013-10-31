package no.steria.bigdatapoc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class Database {
    private static final String keyspaceName = "POC";
    private static final String tableName = keyspaceName + ".power";
    private Session session;

    private static class DatabaseHolder {
        private static final Database instance = new Database();
    }

    public static Database getInstance() {
        return DatabaseHolder.instance;
    }

    public void setUp() {
        Cluster cluster = new Cluster.Builder().addContactPoints("192.168.0.1").build();
        session = cluster.connect();

        Metadata metadata = cluster.getMetadata();
        System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));
        session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }");
        System.out.println("Keyspace " + keyspaceName + " created");
        session.execute("CREATE TABLE " + tableName + "(maalenr text PRIMARY KEY, forbruk text, frekvens text, postnr text)");
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
