import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

public class MainClass {
    public static void main(String[] args) {
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider);

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://dsps-assignment2/emr-tests.jar\n") // This should be a full map reduce application.
                .withArgs("-input", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data",
                        "-output", "s3://dsps-assignment2/output/");

        StepConfig stepConfig = new StepConfig()
                .withName("1wordcount")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M5Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M5Xlarge.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("my_key3")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("wordcount4")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3://dsps-assignment2/logs/");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        runFlowRequest.setAmiVersion("5.28.1");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
