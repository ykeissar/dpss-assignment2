import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapTwo extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text occurrences = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        if (splits.length >= 3) {
            word.set(splits[0]);
            occurrences.set("*_" + splits[2]);
            context.write(word, occurrences);
        }
    }
}

