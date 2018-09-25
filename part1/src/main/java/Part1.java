import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

//import java.util.MiscUtils;

public class Part1 extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Part1.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Part1(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());
        Path path = new Path(args[0]);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus[] status = fs.listStatus(path);
        for(FileStatus file : status) {
            if (!file.getPath().getName().endsWith(".txt")) {
                continue;
            } else {
                Path curPath = new Path(args[0] + "/" + file.getPath().getName());
                FileInputFormat.addInputPath(job, curPath);
            }
        }


//        List<String> lines = Files.readAllLines(Paths.get(args[0]), StandardCharsets.UTF_8);
//        // Use TextInputFormat, the default unless job.setInputFormatClass is used
//        for (String line: lines) {
//            if (!line.isEmpty())
//                FileInputFormat.addInputPath(job, new Path(line));
//        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.getConfiguration().setBoolean("wordcount.case.sensitive", false);
        job.getConfiguration().set("path.stopwords.file", args[2]);
        LOG.info("Stopwords file path: " + args[2]);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        private Set<String> stopwords;
        private boolean caseSensitive = false;
        private static final Pattern BOUNDARY = Pattern.compile("\\s*\\b\\s*");


        protected void setup(Context context)
                throws IOException,
                InterruptedException {
            Configuration config = context.getConfiguration();
            this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);

            String file;
            stopwords = new HashSet<>();
            if ((file = config.get("path.stopwords.file")) != null) {
                Path path = new Path(file);
                FileSystem fs = FileSystem.get(config);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line ;
                while ((line = br.readLine()) != null) {
                    LOG.info("Stopwords: " + line);
                    String[] bagOfStopwords = line.split(",");
                    for (String stopword: bagOfStopwords) {
                        stopwords.add(stopword);
                    }
                }

            }
        }

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            if (!caseSensitive) {
                line = line.toLowerCase();
            }
            Text currentWord = new Text();
            for (String word : BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }

                // Remove special characters e.g. ',' or '.'
                word = word.replaceAll("\\W","");

                // Minimum length of 5
                if (stopwords.contains(word) || word.length() < 5)
                    continue;

                currentWord = new Text(word);
                context.write(currentWord,one);

            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private HashMap<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable count : counts) {
                sum += count.get();
            }
//            context.write(word, new IntWritable(sum));
            countMap.put(new Text(word), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            //java.util.Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);

            TreeMap<Integer, List<Text>> sortedMap = new TreeMap<>();
            for (Text key : countMap.keySet()) {
                int value = countMap.get(key).get();
                if (!sortedMap.containsKey(value)) {
                    sortedMap.put(value, new ArrayList<Text>());
                }
                sortedMap.get(value).add(key);
            }
            int counter = 0;
            boolean done = false;
            for (int key : sortedMap.descendingKeySet()) {
                List<Text> list = sortedMap.get(key);
                for (Text text : list) {
                    context.write(text, new IntWritable(key));
                    counter++;
                    if (counter == 20) {
                        done = true;
                        break;
                    }
                }
                if (done) {
                    break;
                }
            }
        }
    }
}
