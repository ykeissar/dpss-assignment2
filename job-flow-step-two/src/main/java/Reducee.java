import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducee extends Reducer<Text, Text, Text, DoubleWritable> {
    private Text w123 = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long n1 = 0;
        long n2 = 0;
        long n3 = 0;
        long c0 = 1;
        long c1 = 1;
        long c2 = 1;

        for (Text value : values) {
            String str = value.toString();
            String[] split = str.split("_");
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
                    if (wordsSplite.length > 1) {
                        w123.set(wordsSplite[1]);
                    }
                } else if ("0".equals(split[0])) {
                    c0 = Long.parseLong(split[1]);
                }

            }

        }
        double k2 = (Math.log10(n2 + 1) + 1) / (Math.log10(n2 + 1) + 2);
        double k3 = (Math.log10(n3 + 1) + 1) / (Math.log10(n3 + 1) + 2);
        double sum = (k3 * n3 / c2) + ((1 - k3) * k2 * n2 / c1) + ((1 - k3) * (1 - k2) * n1 / c0);
        context.write(w123, new DoubleWritable(sum));
    }
}

