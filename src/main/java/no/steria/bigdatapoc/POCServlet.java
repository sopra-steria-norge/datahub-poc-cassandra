package no.steria.bigdatapoc;

import com.datastax.driver.core.*;
import org.joda.time.DateTime;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class POCServlet extends HttpServlet {
    private final Session session = Database.getInstance().getSession();
    private static final String keyspaceName = Database.keyspaceName;
    private static final String tableName = keyspaceName + ".power";

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Big data POC</h1>");
        insert();
        select();
    }

    public void insert() {
        PreparedStatement insert = session.prepare("INSERT INTO " + tableName + "(timestamp, stationid, kw, council) VALUES (?, ?, ?, ?)");

        BatchStatement batch = new BatchStatement();
        batch.add(insert.bind(new DateTime("2013-01-01T00:45:00.000+01:00").toDate(), "0118RH467", 50.1d, "0118"));
        session.execute(batch);
    }

    public void select() {
        PreparedStatement select = session.prepare("SELECT * FROM " + tableName + " where maalenr = ?");
        BoundStatement boundStatement = new BoundStatement(select);
        boundStatement.bind("00001");
        for (Row row : session.execute(boundStatement).all()) {
            System.out.println(row);
        }
    }


}
