package no.steria.bigdatapoc;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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
        PreparedStatement insert = session.prepare("INSERT INTO " + tableName + "(maalenr, forbruk, frekvens, postnr) VALUES (?, ?, ?, ?)");

        BatchStatement batch = new BatchStatement();
        batch.add(insert.bind("00001", "10", "50.1", "1523"));
        batch.add(insert.bind("00002", "15", "50", "1337"));
        batch.add(insert.bind("00003", "20", "49.9", "1523"));
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
