import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

public class Reducee extends Reducer<Text, Text, Text, DoubleWritable> {
    public static String BUCKET_NAME = "dsps-assignment2";
    public static String KEY = "c0";
    private long c0 = Long.parseLong(downloadFile(BUCKET_NAME, KEY));

    private LinkedList<WordPairs> list = new LinkedList<WordPairs>();
    private String curWords = "";


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long n1 = 0;
        long n2 = 0;
        long n3 = 0;
        long c1 = 1;
        long c2 = 1;
        String[] tripleSplit = new String[3];
        String w123 = "";

        for (Text value : values) {
            String str = value.toString();
            String[] split = str.split("_##_");
            if (split.length > 1) {
                if ("2".equals(split[0])) {
                    c1 = Long.parseLong(split[1]);
                } else if ("3".equals(split[0])) {
                    n1 = Long.parseLong(split[1]);
                } else if ("12".equals(split[0])) {
                    c2 = Long.parseLong(split[1]);
                } else if ("23".equals(split[0])) {
                    n2 = Long.parseLong(split[1]);
                } else if ("123".equals(split[0].substring(0, Math.max(0, split[0].indexOf("@"))))) {
                    n3 = Long.parseLong(split[1]);
                    String[] wordsSplite = split[0].split("@");
                    tripleSplit = wordsSplite[1].split(" ");
                    if (wordsSplite.length > 1) {
                        w123 = wordsSplite[1];
                    }

                }
            }

        }

        double k2 = (Math.log10(n2 + 1) + 1) / (Math.log10(n2 + 1) + 2);
        double k3 = (Math.log10(n3 + 1) + 1) / (Math.log10(n3 + 1) + 2);
        double sum = (k3 * n3 / c2) + ((1 - k3) * k2 * n2 / c1) + ((1 - k3) * (1 - k2) * n1 / c0);
        String w12 = tripleSplit[0] + " " + tripleSplit[1];
        if (!w12.equals(curWords)) {
            curWords = w12;
            writeList(list, context);
        }
        insertPair(list, new WordPairs(sum, w123));


    }

    public static String downloadFile(String bucketName, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        String result = s3Client.getObjectAsString(bucketName, key);

        return result;
    }

    public static void writeList(LinkedList<WordPairs> list, Context context) throws IOException, InterruptedException {
        while (!list.isEmpty()) {
            WordPairs wp = list.getFirst();
            list.removeFirst();
            context.write(wp.getWords(), wp.getProb());
        }
    }

    public static void insertPair(LinkedList<WordPairs> list, WordPairs wp) {
        int count = 0;
        while (list.size() > count) {
            if (list.get(count).compareTo(wp) < 0) {
                break;
            }
            count++;
        }
        list.add(count, wp);

    }

    public static class WordPairs implements Comparable {
        private double prob;
        private String words;

        public WordPairs(double prob, String words) {
            this.prob = prob;
            this.words = words;
        }

        public int compareTo(Object o) {
            if (!(o instanceof WordPairs))
                throw new IllegalArgumentException();
            return Double.compare(prob, ((WordPairs) o).prob);
        }

        public DoubleWritable getProb() {
            return new DoubleWritable(prob);
        }

        public Text getWords() {
            return new Text(words);
        }
    }
}

