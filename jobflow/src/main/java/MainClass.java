import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class MainClass {
    public static class MapperClassOne extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class MapperClassTwo extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class MapperClassThree extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) {
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
//        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider);
//
//        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
//                .withJar("s3n://yoavkei/yourfile.jar") // This should be a full map reduce application.
//                .withMainClass("some.pack.MainClass")
//                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");
//
//        StepConfig stepConfig = new StepConfig()
//                .withName("stepname")
//                .withHadoopJarStep(hadoopJarStep)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");
//
//        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
//                .withInstanceCount(2)
//                .withMasterInstanceType(InstanceType.M1Small.toString())
//                .withSlaveInstanceType(InstanceType.M1Small.toString())
//                .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
//                .withKeepJobFlowAliveWhenNoSteps(false)
//                .withPlacement(new PlacementType("us-east-1a"));
//
//        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
//                .withName("jobname")
//                .withInstances(instances)
//                .withSteps(stepConfig)
//                .withLogUri("s3n://yourbucket/logs/");
//
//        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
//        String jobFlowId = runJobFlowResult.getJobFlowId();
//        System.out.println("Ran job flow with id: " + jobFlowId);
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_EAST_1)
                .build();
        S3Object object = s3.getObject(new GetObjectRequest("datasets.elasticmapreduce", "ngrams/books/20090715/eng-us-all/1gram/data"));
        S3ObjectInputStream inputStream = object.getObjectContent();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String text = "";
        String temp = "";

        try {
            while ((temp = bufferedReader.readLine()) != null) {
                text = text + temp;
            }
            bufferedReader.close();
            inputStream.close();
        } catch (IOException e) {
            System.out.println(new StringBuilder("Exception while downloading from key")
                    .append("")
                    .append(" with reading buffer, Error: ")
                    .append(e.getMessage())
                    .toString());
        }
    }
}
