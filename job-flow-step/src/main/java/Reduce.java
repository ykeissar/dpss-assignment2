import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    public static String BUCKET_NAME = "dsps-assignment2";

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String strKey = key.toString();
        StringBuilder valueToReturn = new StringBuilder();
        List<String> usedValues = new ArrayList<String>();
        long occurrences = 0;

        if (!strKey.equals("$#$")) {
            for (Text value : values) {
                String strValue = value.toString();
                String[] splitValue = strValue.split("_");//w123_role
                if (splitValue[0].equals("*"))
                    occurrences += Long.parseLong(splitValue[1]);
                else {
                    if (!usedValues.contains(strValue)) {
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
                    c += Long.parseLong(splitValue[1]);
//                else {
//                    valueToReturn.append(" ").append(strValue);
//                }

            }
            //String toReturn = valueToReturn.toString();
            uploadFile(String.valueOf(c));
//            Text t = new Text();
//            t.set(ss);
//            context.write(key, t);
        }
    }

    private static void uploadFile(String cont) {
        try {
            String key = "c0";
            AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build().putObject(BUCKET_NAME, key, cont);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    public static void main(String[] args) {
        uploadFile("blabla");

    }


}
