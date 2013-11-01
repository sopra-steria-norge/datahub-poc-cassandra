package no.steria.bigdatapoc;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

public class Main {
    public static void main(String[] args) throws Exception {
        Database.getInstance().setUp();

        Server server = new Server(8080);
        server.setHandler(new WebAppContext("src/main/webapp", "/"));
        server.start();
        System.out.println(httpPost("http://192.168.0.3:9000/push/start", "{\"intervalCount\":35000,\"measurementFrequencyMin\":15,\"sendDelaySec\":0,\"startDate\":\"2013-01-01\",\"url\":\"http://192.168.0.1:8080/data\",\"councilFilter\":\"\",\"parallel\":1, \"dataPerCall\":1000}"));
        server.join();
    }

    private static String httpPost(String url, String answer) throws Exception {
        URLConnection conn = new URL(url).openConnection();
        conn.setRequestProperty("Content-Type", "text/json; charset=utf-8");
        conn.setDoOutput(true);
        try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(conn.getOutputStream(),"utf-8"))) {
            printWriter.append(answer);
        }
        return toString(conn.getInputStream());
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
