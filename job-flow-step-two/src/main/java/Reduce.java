import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int n1 = 0;
        int n2 = 0;
        int n3 = 0;
        int c0 = 0;
        int c1 = 0;
        int c2 = 0;
        Text w123 = new Text();

        for (Text value : values) {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                String[] split = str.split("_");
                if ("2".equals(split[0])) {
                    c1 = Integer.parseInt(split[1]);
                } else if ("3".equals(split[0])) {
                    n1 = Integer.parseInt(split[1]);
                } else if ("12".equals(split[0])) {
                    c2 = Integer.parseInt(split[1]);
                } else if ("23".equals(split[0])) {
                    n2 = Integer.parseInt(split[1]);
                } else if ("123".equals(split[0].substring(0,split[0].indexOf("$")))) {
                    n3 = Integer.parseInt(split[1]);
                    String[] wordsSplite = split[0].split("$");
                    w123.set(wordsSplite[1]);
                } else if ("0".equals(split[0])) {
                    c0 = Integer.parseInt(split[1]);
                }
            }
        }
        double k2 = (Math.log10(n2+1)+1)/(Math.log10(n2+1)+2);
        double k3 = (Math.log10(n3+1)+1)/(Math.log10(n3+1)+2);

        double sum = (k3*n3/c2)+((1-k3)*k2*n2/c1)+((1-k3)*(1-k2)*n1/c0);
        context.write(w123,new DoubleWritable(sum));
    }
}

