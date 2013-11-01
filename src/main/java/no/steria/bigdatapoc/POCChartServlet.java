package no.steria.bigdatapoc;

import com.datastax.driver.core.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
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
    private static final String tableName = keyspaceName + ".power";

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("image/png");
        response.setStatus(HttpServletResponse.SC_OK);
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        dataset.addValue(50, "Frequency", "1");
        dataset.addValue(50.1, "Frequency", "2");
        dataset.addValue(49.8, "Frequency", "3");
        dataset.addValue(49.9, "Frequency", "4");
        dataset.addValue(50, "Frequency", "5");
        dataset.addValue(50.03, "Frequency", "6");
        JFreeChart lineChart = ChartFactory.createLineChart("Usage", "Parameter", "Value", dataset, PlotOrientation.VERTICAL, true, false, false);
        ValueAxis rangeAxis = lineChart.getCategoryPlot().getRangeAxis();
        rangeAxis.setRange(49,51);
        select();
        ChartUtilities.writeBufferedImageAsPNG(response.getOutputStream(), lineChart.createBufferedImage(500, 500));
    }

    public void select() {
        System.out.println("no.steria.bigdatapoc.POCChartServlet.select");
        PreparedStatement select = session.prepare("SELECT * FROM " + tableName + " where stationid = ?");
        select.setConsistencyLevel(ConsistencyLevel.ONE);
        BoundStatement boundStatement = new BoundStatement(select);
        boundStatement.bind("0118WH25");
        for (Row row : session.execute(boundStatement).all()) {
            System.out.println(row);
        }
        System.out.println("done");
    }

}
