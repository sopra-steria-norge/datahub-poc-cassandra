package no.steria.bigdatapoc;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicLong;

public class DataServlet extends HttpServlet {
    private static final AtomicLong counter = new AtomicLong(0);
    private static final AtomicLong counter2 = new AtomicLong(0);
    private Session session = Database.getInstance().getSession();
    private static final String keyspaceName = Database.keyspaceName;
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
        rangeAxis.setRange(49, 51);
        ChartUtilities.writeBufferedImageAsPNG(response.getOutputStream(), lineChart.createBufferedImage(500, 500));
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ServletInputStream inputStream = req.getInputStream();
        String json = toString(inputStream);
        try {
            JSONArray jsonArray = new JSONArray(json);
            PreparedStatement insert = session.prepare("INSERT INTO " + tableName + "(timestamp, stationid, kw, council) VALUES (?, ?, ?, ?)");
            BatchStatement batch = new BatchStatement();
            insert.setConsistencyLevel(ConsistencyLevel.ANY);
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String timeStamp = jsonObject.getString("timeStamp");
                double kw = jsonObject.getDouble("kw");
                String stationId = jsonObject.getString("stationId");
                String council = jsonObject.getString("council");
                batch.add(insert.bind(new DateTime(timeStamp).toDate(), stationId, kw, council));
                counter.incrementAndGet();
            }
            session.execute(batch);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        counter2.incrementAndGet();
        if (counter2.get() % 10 == 0) {
            System.out.println(counter.get());
        }
        resp.setStatus(HttpServletResponse.SC_OK);
    }

    private static String toString(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"))) {
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            return result.toString();
        }
    }
}
