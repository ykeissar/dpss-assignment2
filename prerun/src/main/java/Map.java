import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text word = new Text();
    private LongWritable occurrences = new LongWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        if (splits.length >= 3) {
            word.set(splits[0]);
            occurrences.set(Long.parseLong(splits[2]));
            context.write(word, occurrences);
        }
    }
}

