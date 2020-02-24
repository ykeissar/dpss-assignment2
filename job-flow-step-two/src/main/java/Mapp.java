import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapp extends Mapper<LongWritable, Text, Text, Text> {
    private Text tripleKey = new Text();
    private Text tripleValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        if (splits.length > 1) {
            String strKey = splits[0];
            String occurrences = "";

            if (!strKey.equals("#")) {
                String[] splitValue = splits[1].split(" ");
                if (splitValue.length > 0) {

                    occurrences = splitValue[0];
                    for (int i = 1; i < splitValue.length; i++) {
                        String[] split2 = splitValue[i].split("_");
                        if (split2.length > 1) {

                            tripleKey.set(split2[0]);
                            if (split2[1].equals("123")) {
                                tripleValue.set(split2[1] + "@" + strKey + "_" + occurrences);
                                context.write(tripleKey, tripleValue);
                            } else {
                                tripleValue.set(split2[1] + "_" + occurrences);
                                context.write(tripleKey, tripleValue);
                            }
                        }
                    }
                }
            } else {
                String[] splitValue = splits[1].split(" ");
                if (splitValue.length > 0) {
                    tripleValue.set("0_" + splitValue[0]); //occurrences
                    for (int i = 1; i < splitValue.length; i++) {
                        tripleKey.set(splitValue[i]);
                        context.write(tripleKey, tripleValue);
                    }
                }
            }
        }
    }
}


