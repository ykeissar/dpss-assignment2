import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SeqTry {
    public static void main(String[] args) throws IOException {
        readSeqFile(new Path("/Users/yoav.keissar/Downloads/data"));
    }

    public static void readSeqFile(Path pathToFile) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, pathToFile, conf);

        Text key = new Text(); // this could be the wrong type
        Text val = new Text(); // also could be wrong

        while (reader.next(key, val)) {
            System.out.println(key + ":" + val);
        }
    }
}
