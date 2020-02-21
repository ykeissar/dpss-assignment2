import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String occurrences = "";
        String strKey = "";
        StringBuilder valueToReturn = new StringBuilder();
        StringTokenizer itrKey = new StringTokenizer(key.toString());

        while (itrKey.hasMoreTokens()) {
            strKey = itrKey.nextToken();
        }

        if (!strKey.equals("#")) {
            for (Text value : values) {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    String strValue = itr.nextToken();
                    String[] splitValue = strValue.split("_");
                    if (splitValue[0].equals("*"))
                        occurrences = splitValue[1];
                    else
                        valueToReturn.append(" " + strValue);

                }
            }
            context.write(key, new Text(occurrences + valueToReturn.toString()));
        } else {
            int c0 = 0;
            for (Text value : values) {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    String strValue = itr.nextToken();
                    String[] splitValue = strValue.split("_");
                    if (splitValue[0].equals("*"))
                        c0 += Integer.parseInt(splitValue[1]);
                    else {
                        valueToReturn.append(" " + strValue);
                    }
                }
            }
            context.write(key, new Text(c0 + valueToReturn.toString()));

        }
    }
}

