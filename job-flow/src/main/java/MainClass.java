import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

public class MainClass {
    public static final String PRERUN_ADDRESS = "s3://dsps-assignment2/prerun-output";
    public static final String FIRST_STEP_ADDRESS = "s3://dsps-assignment2/first-output";
    public static final String SECOND_STEP_ADDRESS = "s3://dsps-assignment2/second-output";

    public static void main(String[] args) {
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider);

        HadoopJarStepConfig prerunJarStep = new HadoopJarStepConfig()
                .withJar("s3://dsps-assignment2/prerun.jar")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
                        PRERUN_ADDRESS);

        StepConfig prerunConfig = new StepConfig()
                .withName("prerun")
                .withHadoopJarStep(prerunJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopFirstJarStep = new HadoopJarStepConfig()
                .withJar("s3://dsps-assignment2/job-flow-step.jar")
                .withArgs(PRERUN_ADDRESS + "/part-r-00000", PRERUN_ADDRESS + "/part-r-00001", PRERUN_ADDRESS + "/part-r-00002", PRERUN_ADDRESS + "/part-r-00003", PRERUN_ADDRESS + "/part-r-00004",
                        FIRST_STEP_ADDRESS);

        StepConfig firstStepConfig = new StepConfig()
                .withName("mapreduceone")
                .withHadoopJarStep(hadoopFirstJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopSecondJarStep = new HadoopJarStepConfig()
                .withJar("s3://dsps-assignment2/job-flow-step-two.jar")
                .withArgs(FIRST_STEP_ADDRESS + "/part-r-00000", FIRST_STEP_ADDRESS + "/part-r-00001", FIRST_STEP_ADDRESS + "/part-r-00002", FIRST_STEP_ADDRESS + "/part-r-00003", FIRST_STEP_ADDRESS + "/part-r-00004",
                        SECOND_STEP_ADDRESS);

        StepConfig secondStepConfig = new StepConfig()
                .withName("mapreducetwo")
                .withHadoopJarStep(hadoopSecondJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(4)
                .withMasterInstanceType(InstanceType.M5Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M5Xlarge.toString())
                .withHadoopVersion("2.8.5").withEc2KeyName("my_key3")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("dsps_assignment2")
                .withInstances(instances)
                .withSteps(prerunConfig, firstStepConfig, secondStepConfig)
                .withReleaseLabel("emr-5.29.0")
                .withLogUri("s3://dsps-assignment2/logs/");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        mapReduce.runJobFlow(runFlowRequest);

        //String jobFlowId = runJobFlowResult.getJobFlowId();
        //System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
