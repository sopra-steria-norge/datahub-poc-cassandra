package no.steria.bigdatapoc;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

public class DataServlet extends HttpServlet {

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
        rangeAxis.setRange(49, 51);
        ChartUtilities.writeBufferedImageAsPNG(response.getOutputStream(), lineChart.createBufferedImage(500, 500));
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ServletInputStream inputStream = req.getInputStream();
        String json = toString(inputStream);
        try {
            JSONArray jsonArray = new JSONArray(json);
            System.out.println(jsonArray.length());
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                System.out.println(jsonObject);
                System.out.println("timestamp:" + jsonObject.getString("timeStamp"));
                System.out.println("kw:" + jsonObject.getDouble("kw"));
                System.out.println("stationId:" + jsonObject.getString("stationId"));
                System.out.println("council:" + jsonObject.getString("council"));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        resp.setStatus(HttpServletResponse.SC_OK);
    }

    private static String toString(InputStream inputStream) throws IOException {
        try (Reader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"))) {
            StringBuilder result = new StringBuilder();
            int c;
            while ((c = reader.read()) != -1) {
                result.append((char)c);
            }
            return result.toString();
        }
    }
}
