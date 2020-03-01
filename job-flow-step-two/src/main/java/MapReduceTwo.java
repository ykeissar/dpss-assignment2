import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceTwo {
    /**
     * inputs:
     * arg[0..args.length-2]
     * outputs:
     * args[args.length-1]
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("mapreducetwo");
        job.setJarByClass(MapReduceTwo.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setReducerClass(Reducee.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);

        for (int i = 0; i < args.length - 1; i++)
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, Mapp.class);

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.waitForCompletion(true);
    }
}
