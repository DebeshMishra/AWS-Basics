package com.amazonaws.samples;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Main
{
    private static final AWSCredentials AWS_CREDENTIALS;
    
    static {
        // Your accesskey and secretkey
        AWS_CREDENTIALS = new BasicAWSCredentials(
                "AKIAR2FYKC34HHPTDNHK",
                "EK0rrGTBVJK/OsgPa1zV/+qpfPZTTOrRM9ZgeqqI"
        );
    }
    
    public static String createAndRunEC2Instance(AmazonEC2 ec2Client) {
         
        // Launch an Amazon EC2 Instance
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest().withImageId("ami-00ddb0e5626798373")
                .withInstanceType("t2.micro")
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName("debeshaws")						//Enter your key pair name
                .withSecurityGroupIds("sg-024a97e0e7db9abba"); 	//Enter your security group id here
 
        RunInstancesResult runInstancesResult = ec2Client.runInstances(runInstancesRequest);
 
        Instance instance = runInstancesResult.getReservation().getInstances().get(0);
        String instanceId = instance.getInstanceId();
 
        // Setting up the tags for the instance
        CreateTagsRequest createTagsRequest = new CreateTagsRequest()
                .withResources(instance.getInstanceId())
                .withTags(new Tag("Name", "AWS Test"));			//Enter your tags here
        ec2Client.createTags(createTagsRequest);
 
        // Starting the Instance
        StartInstancesRequest startInstancesRequest = new StartInstancesRequest().withInstanceIds(instanceId);
 
        ec2Client.startInstances(startInstancesRequest);
        
        return instanceId;
    }
    
    public static void terminateInstance(String instance_id, AmazonEC2 ec2) {
    	
    	TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                .withInstanceIds(instance_id);
        ec2.terminateInstances(terminateInstancesRequest)
                .getTerminatingInstances()
                .get(0)
                .getPreviousState()
                .getName();
    }

    public static String createBucket(String bucket_name) {
    	// creating S3 instance
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1)
        		.build();
        
        try {
        	s3.createBucket(bucket_name);
        } 
        catch (AmazonS3Exception e) {
        	System.err.println(e.getErrorMessage());
        }
        
        return bucket_name;
    }
    
    public static void deleteBucket(String bucket_name) {
    	// creating S3 instance
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1)
        		.build();
        
        try {
        	s3.deleteBucket(bucket_name);
        } 
        catch (AmazonS3Exception e) {
        	System.err.println(e.getErrorMessage());
        }
    }
    
    public static void uploadFile(String bucket_name) {
    	// creating S3 instance
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1)
        		.build();
        
        try {
        	s3.putObject(bucket_name, "CSE546test.txt", " ");
        	System.out.println("Successfully added the file CSE546test.txt");
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
    }
    
    public static void deleteFile(String bucket_name) {
    	// creating S3 instance
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1)
        		.build();
        
        try {
        	s3.deleteObject(bucket_name, "CSE546test.txt");
        }
        catch(AmazonServiceException e) {
        	e.printStackTrace();
        }
    }
    
    public static String createSQS() {
    	// creating SQS instance
        final AmazonSQS sqs = AmazonSQSClient.builder()
        		.withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
        		.withRegion(Regions.US_EAST_1)
        		.build();
        
        Map<String, String> queueAttributes = new HashMap<>();
        queueAttributes.put("FifoQueue", "true");
        queueAttributes.put("ContentBasedDeduplication", "true");
        
        CreateQueueRequest createFifoQueueRequest = new CreateQueueRequest("debeshawsqueue.fifo")
        		.withAttributes(queueAttributes);
        
        String queueURL = sqs.createQueue(createFifoQueueRequest)
        		  .getQueueUrl();
        
        return queueURL;
    }
    
    public static void sendMessage(String queueURL) {
    	// creating SQS instance
        final AmazonSQS sqs = AmazonSQSClient.builder()
        		.withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
        		.withRegion(Regions.US_EAST_1)
        		.build();
    	
    	Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
    	messageAttributes.put("Title", new MessageAttributeValue()
    	  .withStringValue("test message")
    	  .withDataType("String")); 
    	
    	SendMessageRequest sendMessageFifoQueue = new SendMessageRequest()
    			  .withQueueUrl(queueURL)
    			  .withMessageBody("This is a test message")
    			  .withMessageGroupId("debesh-group-1")
    			  .withMessageAttributes(messageAttributes);
    	
    	sqs.sendMessage(sendMessageFifoQueue);
    	System.out.println("Message sent");
    }
    
    public static List<Message> receiveMessage(String queueURL) {
    	// creating SQS instance
        final AmazonSQS sqs = AmazonSQSClient.builder()
        		.withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
        		.withRegion(Regions.US_EAST_1)
        		.build();
    	
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL)
        		  .withWaitTimeSeconds(10)
        		  .withMaxNumberOfMessages(10);

        List<Message> sqsMessages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("Title")).getMessages();
        
        return sqsMessages;
    }
    
    public static void main(String[] args) {
    	
    	// Set up the amazon ec2 client
        AmazonEC2 ec2Client = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
                .withRegion(Regions.US_EAST_1)
                .build();
        
        // Set up the amazon s3 client
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1)
        		.build();
        
        // creating SQS instance
        AmazonSQS sqs = AmazonSQSClient.builder()
        		.withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
        		.withRegion(Regions.US_EAST_1)
        		.build();
        
        //creating EC2 instance
        String ec2InstanceId = createAndRunEC2Instance(ec2Client);
        
        //creating S3 bucket
        String bucketName = createBucket("debeshawsbucket");
        
        //creating SQS FIFO queue
        String queueUrl = createSQS();
        
        //1 min timer
    	try {
    		System.out.println("Request sent, wait for 1 min");
			Thread.sleep(40000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	//Displaying resources
    	DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(ec2InstanceId);
    	DescribeInstancesResult describeInstancesResult = ec2Client.describeInstances(describeInstancesRequest);
    	Instance instance = describeInstancesResult.getReservations().get(0).getInstances().get(0);   	
    	System.out.println("Your Amazon EC2 Instance "+ec2InstanceId+" is "+instance.getState().getName());

        List<Bucket> buckets = s3.listBuckets();
        System.out.println("Your Amazon S3 Bucket is "+buckets.get(0).getName());
        
    	ListQueuesResult lq_result = sqs.listQueues();
    	System.out.println("Your Amazon SQS Queue is "+lq_result.getQueueUrls());
    	
    	//Uploading empty file to S3 bucket
    	uploadFile(bucketName);
    	
    	//Sending a message to SQS Queue
    	sendMessage(queueUrl);
    	
    	//Checking number of messages before polling
    	GetQueueAttributesResult attributes = sqs.getQueueAttributes(queueUrl, Collections.singletonList("ApproximateNumberOfMessages"));
        System.out.println("Number of Messages in Queue "+attributes.getAttributes().get("ApproximateNumberOfMessages"));
        
        //Polling and the deleting the message
        List<Message> messages = receiveMessage(queueUrl);
        sqs.deleteMessage(new DeleteMessageRequest()
      		  .withQueueUrl(queueUrl)
      		  .withReceiptHandle(messages.get(0).getReceiptHandle()));
        
        try {
        	System.out.println("Please wait while polling the messages");
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        //Display the messages
        System.out.println("Title: "+messages.get(0).getMessageAttributes().get("Title").getStringValue());
        System.out.println("Body: "+messages.get(0).getBody());
        
        //Checking number of messages after polling
        GetQueueAttributesResult attributes1 = sqs.getQueueAttributes(queueUrl, Collections.singletonList("ApproximateNumberOfMessages"));
        System.out.println("Number of Messages in Queue "+attributes1.getAttributes().get("ApproximateNumberOfMessages"));
        
    	//Deleting resources
        deleteFile(bucketName);
        terminateInstance(ec2InstanceId, ec2Client);
    	deleteBucket(bucketName);
        sqs.deleteQueue(queueUrl);
        
        try {
        	System.out.println("Deleting resources.. Please wait..");
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        //Displaying resources
    	DescribeInstancesRequest describeInstancesRequest1 = new DescribeInstancesRequest().withInstanceIds(ec2InstanceId);
    	DescribeInstancesResult describeInstancesResult1 = ec2Client.describeInstances(describeInstancesRequest1);
    	Instance instance1 = describeInstancesResult1.getReservations().get(0).getInstances().get(0);   	
    	System.out.println("Your Amazon EC2 Instance "+ec2InstanceId+" is "+instance1.getState().getName());

        System.out.println("Your Amazon S3 Bucket is "+s3.listBuckets());
        
    	ListQueuesResult lq_result1 = sqs.listQueues();
    	System.out.println("Your Amazon SQS Queue is "+lq_result1.getQueueUrls());
       
    	System.out.println("END OF PROGRAM");
    }
}
