package no.steria.bigdatapoc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class Database {
    public static final String keyspaceName = "POCJAN";
    private static final String station = keyspaceName + ".station";
    private static final String council = keyspaceName + ".council";
    private static final String stationDay = keyspaceName + ".station_day";
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
//        session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
//        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }");
////        System.out.println("Keyspace " + keyspaceName + " created");
//        session.execute("CREATE TABLE " + station + "(timestamp timestamp, stationid text, kw double, council text, " +
//                "PRIMARY KEY(stationid,timestamp))");
//        session.execute("CREATE TABLE " + council + "(tid timestamp, stationid text, kw double, council text, " +
//                "PRIMARY KEY(council,stationid, tid))");
        //       session.execute("CREATE TABLE " + stationDay + "(stationid text, kw double, council text, day text, time text, tid timestamp" +
        //       "PRIMARY KEY((stationid, day), tid))");

    }


    public Session getSession() {
        return session;
    }
}
