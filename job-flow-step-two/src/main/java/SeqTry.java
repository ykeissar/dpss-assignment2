import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class SeqTry {
    public static void main(String[] args) throws IOException {
        readSeqFile("/Users/yoav.keissar/Downloads/shir_data");
        //writeSequnce("/Users/yoav.keissar/Dowloads/in", "/Users/yoav.keissar/Dowloads/out");
    }

    public static void readSeqFile(String path) throws IOException {
        Configuration conf = new Configuration();
        try {
            Path inFile = new Path(path);
            SequenceFile.Reader reader = null;
            try {
                LongWritable key = new LongWritable();
                Text value = new Text();
                reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(inFile), SequenceFile.Reader.bufferSize(4096));
                //System.out.println("Reading file ");
                while (reader.next(key, value)) {
                    System.out.println("Key: " + key + " Value: " + value);
                }

            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void writeSequnce(String inputPath, String outputPath) {
        Configuration conf = new Configuration();
        int i = 0;
        try {
            FileSystem fs = FileSystem.get(conf);
            // input file in local file system
            File file = new File(inputPath);
            // Path for output file
            Path outFile = new Path(outputPath);
            IntWritable key = new IntWritable();
            Text value = new Text();
            SequenceFile.Writer writer = null;
            try {
                writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outFile),
                        SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()),
                        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new GzipCodec()));
                for (String line : FileUtils.readLines(file)) {
                    key.set(i++);
                    value.set(line);
                    writer.append(key, value);
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

