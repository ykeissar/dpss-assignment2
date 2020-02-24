import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String occurrences = "";
        String strKey = key.toString();
        StringBuilder valueToReturn = new StringBuilder();

        if (!strKey.equals("#")) {
            for (Text value : values) {
                String strValue = value.toString();
                String[] splitValue = strValue.split("_");
                if (splitValue[0].equals("*"))
                    occurrences = splitValue[1];
                else
                    valueToReturn.append(" ").append(strValue);


            }
            context.write(key, new Text(occurrences + valueToReturn.toString()));
        } else {
            long c0 = 0;
            for (Text value : values) {
                String strValue = value.toString();
                String[] splitValue = strValue.split("_");
                if (splitValue[0].equals("*"))
                    c0 +=Long.parseLong(splitValue[1]);
                else {
                    valueToReturn.append(" ").append(strValue);
                }

            }
            context.write(key, new Text(c0 + valueToReturn.toString()));

        }
    }
}
