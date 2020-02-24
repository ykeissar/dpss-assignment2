import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String strKey = key.toString();
        StringBuilder valueToReturn = new StringBuilder();
        List<String> usedValues = new ArrayList<String>();
        long occurrences=0;

        if (!strKey.equals("#")) {
            for (Text value : values) {
                String strValue = value.toString();
                String[] splitValue = strValue.split("_");
                if (splitValue[0].equals("*"))
                    occurrences += Long.parseLong(splitValue[1]);
                else {
                    if(!usedValues.contains(strValue)) {
                        valueToReturn.append(" ").append(strValue);
                        usedValues.add(strValue);
                    }
                }
            }
            context.write(key, new Text(occurrences + valueToReturn.toString()));
        } else {
            long c = 0;
            for (Text value : values) {
                String strValue = value.toString();
                String[] splitValue = strValue.split("_");
                if (splitValue[0].equals("*"))
                    c +=Long.parseLong(splitValue[1]);
                else {
                    valueToReturn.append(" ").append(strValue);
                }

            }
            String toReturn = valueToReturn.toString();
            String ss = String.valueOf(c);
            Text t = new Text();
            t.set(ss+toReturn);
            context.write(key, t);

        }
    }
}
