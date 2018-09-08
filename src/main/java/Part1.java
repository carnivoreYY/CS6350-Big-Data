import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class Part1 {


    public static void main(String[] args) {
        System.out.println("Please input the directory on HDFS where the files to be stored.");
        Class1 c = new Class1();
        c.setDir();
        System.out.println("host is " + c.getDir());
        String dirDst = c.getDir();
        // download file;
        String[] books = {
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"
        };

        for (String book : books) {
            InputStream input = null;
            OutputStream output = null;

            // write to hdfs
            try {
                URL url = new URL(book);
        // ReadableByteChannel rbc = Channels.newChannel(website.openStream());
        // FileOutputStream fos = new FileOutputStream(FileName);
        // fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

        // download file and upload to HDFS
        input = new BufferedInputStream(url.openStream());

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dirDst), config);
        String dst = dirDst + "/" + FilenameUtils.getName(url.getPath());
        //System.out.println(dst);

        Path dstPath = new Path(dst);

        output = fs.create(new Path(dst), new Progressable() {
                    public void progress() {
                        System.out.print(".");
                    }
                });
        IOUtils.copyBytes(input, output, 4096, true);

        // decompress file and delete initial file;
        CompressionCodecFactory factory = new CompressionCodecFactory(config);
        CompressionCodec codec = factory.getCodec(dstPath);
        if (codec == null) {
            System.err.println("No codec found for " + book);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(dst, codec.getDefaultExtension());

        input = codec.createInputStream(fs.open(dstPath));
        output = fs.create(new Path(outputUri));
        IOUtils.copyBytes(input, output, config);

        if(fs.delete(dstPath, true)){
            System.out.println("Task finished!");
        }else{
            System.out.println("Failed to delete "+ dstPath + "!");
        }

    } catch (Exception e) {
        System.out.print(e.getStackTrace());
    } finally {
        IOUtils.closeStream(input);
        IOUtils.closeStream(output);
    }
}

    }
}
