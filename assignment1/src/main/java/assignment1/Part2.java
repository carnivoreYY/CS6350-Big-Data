package assignment1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.params.HttpParams;
import org.apache.http.client.params.AllClientPNames;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Part2 {
    public static void main(String[] args) {
        String pdst = args[0];
        String link = args[1];
        System.out.println("File URL: " + link);
        System.out.println("HDFS Directory: " + pdst);

        InputStream in = null;
        ZipInputStream zis = null;

        try {
            Configuration conf = new Configuration();
            // download zip file
            //!!!http is not working here;
            String zipUrl = getRealUrl(link);
            in = new URL(zipUrl).openStream();
            zis = new ZipInputStream(in);

            // unzip file and save to text files according to file names;
            ZipEntry ze;
            while ((ze = zis.getNextEntry()) != null) {

                String dst = pdst + "/" +ze.getName();

                FileSystem fs = FileSystem.get(URI.create(dst), conf);

                Path dstPath = new Path(dst);
                OutputStream out = fs.create(dstPath, new Progressable() {
                    public void progress() {
                        System.out.print(".");
                    }
                });

                IOUtils.copyBytes(zis, out, 4096, false);
                IOUtils.closeStream(out);

                // delete original file;
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(zis);
            IOUtils.closeStream(in);
            System.out.println("finish task");
        }
    }

    private static String getRealUrl(String url) {
        try {
            HttpClient client = new HttpClient();
            HttpMethod method = new HeadMethod(url);
            HttpParams params = client.getParams();
            params.setParameter(AllClientPNames.HANDLE_REDIRECTS, false);
            client.executeMethod(method);
            url = method.getURI().getURI();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return url;

    }
}
