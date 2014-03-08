package net.sparktheweb.inflow;

import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.*;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.Socket;
import java.util.Date;

/**
 * Created by glombst on 08/03/2014.
 */
public class SiteCrawler {

    public static void main(String[] args) throws IOException, HttpException {


        final InputStream resourceAsStream = System.class.getResourceAsStream("/top-1m.csv");
        final BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream));

        String tempDir = System.getProperty("java.io.tmpdir");
        final File sparkDir = new File(tempDir + "SparkTheWeb");
        if (!sparkDir.exists()) {
            sparkDir.mkdir();
        }
        System.out.println("Writing to:" + sparkDir.toString());


        String line = null;
        while ((line = reader.readLine()) != null) {
            String url = line.substring(line.indexOf(",") + 1);

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpGet httpget = new HttpGet("http://" + url + "/");
            CloseableHttpResponse response = httpclient.execute(httpget);
            try {
                HttpEntity entity = response.getEntity();
                if (entity != null) {

                    String filename = url + "-" + new Date().toString() + ".html";
                    String completeName = tempDir + "SparkTheWeb" + File.separator + filename;

                    final FileOutputStream fileOutputStream = new FileOutputStream(completeName);

                    entity.writeTo(fileOutputStream);
                    fileOutputStream.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                response.close();
            }
        }
    }
}

