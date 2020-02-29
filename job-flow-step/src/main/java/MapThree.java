import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapThree extends Mapper<LongWritable, Text, Text, Text> {
    private Text w2 = new Text();
    private Text w3 = new Text();
    private Text w23 = new Text();
    private Text w12 = new Text();
    private Text w123 = new Text();
    private Text toReturn = new Text();
    private Text hash = new Text("#");


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        if (splits.length > 0) {
            String[] splitsTriple = splits[0].split(" ");
            if (splitsTriple.length > 2) {

                w2.set(splitsTriple[1]);
                w3.set(splitsTriple[2]);
                w23.set(splitsTriple[1] + " " + splitsTriple[2]);
                w12.set(splitsTriple[0] + " " + splitsTriple[1]);
                w123.set(splits[0]);

                String triple = splitsTriple[0] + "-" + splitsTriple[1] + "-" + splitsTriple[2];

                toReturn.set(triple + "_" + 2);
                context.write(w2, toReturn);

                toReturn.set(triple + "_" + 3);
                context.write(w3, toReturn);

                toReturn.set(triple + "_" + 12);
                context.write(w12, toReturn);

                toReturn.set(triple + "_" + 23);
                context.write(w23, toReturn);

                toReturn.set(triple + "_" + 123);
                context.write(w123, toReturn);

//                toReturn.set(id);
//                context.write(hash, toReturn);

                toReturn.set("*_" + splits[2]);
                context.write(w123, toReturn);
            }
        }
    }
}

