package no.steria.bigdatapoc;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class Main {
    public static void main(String[] args) throws Exception {
        Database database = Database.getInstance();
        database.setUp();
        Server server = new Server(8080);
        server.setHandler(new WebAppContext("src/main/webapp", "/"));
        server.start();
        server.join();
        database.tearDown();
    }
}
