import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapOne extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text occurrences = new Text();
    private Text hash = new Text("$#$");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        if (splits.length >= 3) {
            word.set(splits[0]);
            occurrences.set("*_" + splits[2]);
            context.write(word, occurrences);
            context.write(hash, occurrences);
            if(splits[0].equals("$#$"))
                uploadFile("$#$");
            if(splits[0].equals("#"))
                uploadFile("#");
        }
    }

    private static void uploadFile(String cont) {
        try {
            String key = "c0";
            AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build().putObject("dsps-assignment2", cont, cont);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }
}

