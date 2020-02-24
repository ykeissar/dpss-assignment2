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

        HadoopJarStepConfig hadoopFirstJarStep = new HadoopJarStepConfig()
                .withJar("s3://dsps-assignment2/job-flow-step.jar\n") // This should be a full map reduce application.
                .withArgs("-input", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
                        "-output", "s3://dsps-assignment2/output/");

        StepConfig firstStepConfig = new StepConfig()
                .withName("mapreduceone")
                .withHadoopJarStep(hadoopFirstJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopSecondJarStep = new HadoopJarStepConfig()
                .withJar("s3://dsps-assignment2/job-flow-step-two.jar\n") // This should be a full map reduce application.
                .withArgs("-input", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/",
                        "-output", "s3://dsps-assignment2/output/");

        StepConfig secondStepConfig = new StepConfig()
                .withName("mapreducetwo")
                .withHadoopJarStep(hadoopSecondJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M5Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M5Xlarge.toString())
                .withHadoopVersion("2.8.5").withEc2KeyName("my_key3")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("mapreduceone")
                .withInstances(instances)
                .withSteps(firstStepConfig)
                .withReleaseLabel("emr-5.29.0")
                .withLogUri("s3://dsps-assignment2/logs/");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

        runFlowRequest = new RunJobFlowRequest()
                .withName("mapreducetwo")
                .withInstances(instances)
                .withSteps(secondStepConfig)
                .withReleaseLabel("emr-5.29.0")
                .withLogUri("s3://dsps-assignment2/logs/");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");

        runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);


        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
