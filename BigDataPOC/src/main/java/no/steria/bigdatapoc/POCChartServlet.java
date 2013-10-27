package no.steria.bigdatapoc;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

public class POCChartServlet extends HttpServlet {

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
        ChartUtilities.writeBufferedImageAsPNG(response.getOutputStream(), lineChart.createBufferedImage(500, 500));
    }
}
