package no.steria.bigdatapoc;

import com.datastax.driver.core.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class POCChartServlet extends HttpServlet {
    private static final Session session = Database.getInstance().getSession();
    public static final String keyspaceName = "POCJAN";
    private static final String tableName = keyspaceName + ".stationcouncil";

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("image/png");
        response.setStatus(HttpServletResponse.SC_OK);
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();


        try {
            select(dataset);
            JFreeChart lineChart = ChartFactory.createLineChart("Forbruk", "Tid", "Kw", dataset, PlotOrientation.VERTICAL, true, false, false);
            ChartUtilities.writeBufferedImageAsPNG(response.getOutputStream(), lineChart.createBufferedImage(500, 500));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void select(DefaultCategoryDataset dataset) {
        System.out.println("no.steria.bigdatapoc.POCChartServlet.select");
        PreparedStatement select = session.prepare("SELECT * FROM POCJAN.stasjon_dag where stasjon  = ? and dag = ?");

        select.setConsistencyLevel(ConsistencyLevel.ONE);
        BoundStatement boundStatement = new BoundStatement(select);
        boundStatement.bind("1601RH14055", "2013-01-01");
        for (Row row : session.execute(boundStatement).all()) {
            ;
            dataset.addValue(row.getDouble(4), "1601RH14055", row.getDate(2));
        }
        System.out.println("done");
    }

}
