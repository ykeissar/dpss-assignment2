import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapThree extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private LongWritable occurrences = new LongWritable();
    private Text w2 = new Text();
    private Text w3 = new Text();
    private Text w23 = new Text();
    private Text w12 = new Text();
    private Text w123 = new Text();
    private Text toReturn = new Text();
    private Text hash = new Text("#");



    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String[] splits = itr.nextToken().split(" ");
            w2.set(splits[1]);
            w3.set(splits[2]);
            w23.set(splits[1]+ " "+ splits[2]);
            w12.set(splits[0]+ " "+ splits[1]);
            w123.set(splits[0]+ " "+ splits[1]+ " "+splits[2]);

            toReturn.set(key.toString()+"_"+2);
            context.write(w2,toReturn);

            toReturn.set(key.toString()+"_"+3);
            context.write(w3,toReturn);

            toReturn.set(key.toString()+"_"+12);
            context.write(w12,toReturn);

            toReturn.set(key.toString()+"_"+23);
            context.write(w23,toReturn);

            toReturn.set(key.toString()+"_"+123);
            context.write(w123,toReturn);

            toReturn.set(key.toString());
            context.write(hash,toReturn);

            toReturn.set("*_"+splits[4]);
            context.write(w123,toReturn);
        }
    }
}

