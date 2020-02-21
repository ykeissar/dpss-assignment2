import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import static java.util.Arrays.copyOfRange;

public class Map extends Mapper<Text, Text, Text, Text> {
    private Text tripleKey = new Text();
    private Text tripleValue = new Text();

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itrKey = new StringTokenizer(key.toString());
        String strKey = "";
        while (itrKey.hasMoreTokens()) {
            strKey = itrKey.nextToken();
        }

        StringTokenizer itr = new StringTokenizer(value.toString());
        String occurrences = "";
        if (!strKey.equals("#")) {
            while (itr.hasMoreTokens()) {
                String[] splits = itr.nextToken().split(" ");
                occurrences = splits[0];
                for (int i = 1; i < splits.length; i++) {
                    String[] split2 = splits[i].split("_");
                    tripleKey.set(split2[0]);
                    if (split2[1].equals("123")) {
                        tripleValue.set(split2[1] + "$" + strKey + "_" + occurrences);
                        context.write(tripleKey, tripleValue);
                    } else {
                        tripleValue.set(split2[1] + "_" + occurrences);
                        context.write(tripleKey, tripleValue);
                    }
                }
            }
        } else {
            while (itr.hasMoreTokens()) {
                String[] splits = itr.nextToken().split(" ");
                occurrences = splits[0];
                for (int i = 1; i < splits.length; i++) {
                    tripleKey.set(splits[i]);
                    tripleValue.set("0_" + occurrences);
                    context.write(tripleKey, tripleValue);
                }
            }
        }
    }
}
