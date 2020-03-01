import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapF extends Mapper<LongWritable, Text, Text, Text> {
    private Text w = new Text();
    private Text w2 = new Text();
    private Text w3 = new Text();
    private Text w23 = new Text();
    private Text w12 = new Text();
    private Text w123 = new Text();
    private Text toReturn = new Text();
    private Text occurrences = new Text();
    private Text cTag = new Text("$#$");


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {//key = rowNumber in file, value = w/w1w2/w1w2w3\toccurences
        String[] splits = value.toString().split("\t");
        if (splits.length > 0) {
            String words = splits[0];
            String occur = splits[1];
            String[] wordsSplit = words.split(" ");
            switch (wordsSplit.length) {
                case 1:
                    w.set(words);
                    occurrences.set("*_##_" + occur);
                    context.write(w, occurrences);
                    context.write(cTag, occurrences);
                    break;
                case 2:
                    w.set(words);
                    occurrences.set("*_##_" + occur);
                    context.write(w, occurrences);
                    break;
                case 3:
                    w2.set(wordsSplit[1]);
                    w3.set(wordsSplit[2]);
                    w23.set(wordsSplit[1] + " " + wordsSplit[2]);
                    w12.set(wordsSplit[0] + " " + wordsSplit[1]);
                    w123.set(words);
                    String tripleKey = wordsSplit[0] + "_####_" + wordsSplit[1] + "_####_" + wordsSplit[2];

                    toReturn.set(tripleKey + "_##_" + 2);
                    context.write(w2, toReturn);

                    toReturn.set(tripleKey + "_##_" + 3);
                    context.write(w3, toReturn);

                    toReturn.set(tripleKey + "_##_" + 12);
                    context.write(w12, toReturn);

                    toReturn.set(tripleKey + "_##_" + 23);
                    context.write(w23, toReturn);

                    toReturn.set(tripleKey + "_##_" + 123);
                    context.write(w123, toReturn);

                    toReturn.set("*_##_" + occur);
                    context.write(w123, toReturn);

                    break;
            }
        }
    }
}

