package qm;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.*;

/************************************************************************\
 * Utility class to interact with AWS Simple Queue Service (SQS)
 * 
 * Author: Denis Baltor
\************************************************************************/
public class QueueManager {
  
//  private static final String TEST_QUEUE_URL = "https://sqs.eu-west-2.amazonaws.com/556385395922/test-sqs";
  private static final String TEST_QUEUE_NAME = "test-queue-" + System.currentTimeMillis();
  

  /**
   * The class runs a set of integration tests when executed standalone.
   * 1) Create a test queue
   * 2) Put a test message on the test queue
   * 3) Get the test message from the test queue
   * 4) Delete the test queue
   */
  public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    String testQueueURL = "";
    System.out.println("Testing QueueManager...");
    System.out.println("Creating a new queue: " + TEST_QUEUE_NAME);
    
    try{
      testQueueURL = createQueue(TEST_QUEUE_NAME);
      System.out.println("Created test queue with URL: " + testQueueURL);
    }catch( Exception e) {
      System.out.println("Exception cought while trying creating the test queue...");
      e.printStackTrace();
      System.exit(1);
    }
    
    System.out.println("Press ENTER...");
    sc.nextLine();

    final String TEST_MSG = "Test message number: " + System.currentTimeMillis();

    System.out.println("Testing QueueManager class using queue: " + testQueueURL);
    System.out.println("The following message will be written into the test queue...");
    System.out.println(TEST_MSG);
    
    try {
      put(testQueueURL, TEST_MSG);
    }catch( Exception e) {
      System.out.println("Exception cought while trying writing into the test queue...");
      e.printStackTrace();
      System.exit(1);
    }
    
    System.out.println("Press ENTER...");
    sc.nextLine();
    
    System.out.println("The following messages have been read from the test queue...");
    get(testQueueURL) 
      .stream()
      .map(msg -> msg.body())
      .forEach(System.out::println);
    
    System.out.println("Press ENTER...");
    sc.nextLine();
    
    try{
      deleteQueue(testQueueURL);
      System.out.println("Deleted queue: " + TEST_QUEUE_NAME);
    }catch( Exception e) {
      System.out.println("Exception cought while trying deleting the test queue...");
      e.printStackTrace();
      System.exit(1);
    }
    
    System.out.println();
    System.out.println("*******************");
    System.out.println("ALL TESTS PASSED!!!");
    System.out.println("*******************");
  }
  
  /**
   * Build a SQS client using London region
   * 
   * @return The client built
  */
  private static SqsClient buildClient() {
    // Build a SQS service client
    return SqsClient.builder()
      .region(Region.EU_WEST_2)
      .build();      
  }
  
  /**
   * Create a new queue
   * 
   * @param queueName Name of the queue to be created
   * @return The new queue's URL
   * @throws Exception
   */    
  public static String createQueue(String queueName) throws Exception{
    return buildClient().createQueue(
                              CreateQueueRequest.builder()
                                .queueName(queueName)
                                .build())
      .queueUrl();  
  }

  /**
   * Delete the queue provided
   * 
   * @param queueUrl URL of the queue to be deleted
   * @throws Exception
   */     
  public static void deleteQueue(String queueUrl) throws Exception{
    buildClient().deleteQueue(
                              DeleteQueueRequest.builder()
                                .queueUrl(queueUrl)
                                .build());
  }
  

  
  /**
   * Put the given message into the queue provided
   * 
   * @param queueUrl Target queue's URL
   * @param messageBody Content to be sent
   * @return The message id generated
   * @throws Exception
   */  
  public static String put(String queueUrl, String messageBody) throws Exception{
    return buildClient().sendMessage(SendMessageRequest.builder()
                                   .queueUrl(queueUrl)
                                   .messageBody(messageBody)
                                   .build())
      .messageId();    
  }
 
  /**
   * Put the given message into the queue provided
   * 
   * @param queueUrl Target queue's URL
   * @param messageBody Content to be sent
   * @param delaySeconds Time to wait before sending the message
   * @return The message id generated
   * @throws Exception
   */   
  public static String put(String queueUrl, String messageBody, Integer delaySeconds) throws Exception{ 
    return buildClient().sendMessage(SendMessageRequest.builder()
                                   .queueUrl(queueUrl)
                                   .messageBody(messageBody)
                                   .delaySeconds(delaySeconds)
                                   .build())
      .messageId();
  }
  
  /**
   * Retrieve up to 5 messages from the queue provided.
   * The queue will be polled up to 20 seconds.
   * All message attributes will be retrieved.
   * 
   * @param queueUrl Target queue's URL
   * @return The list of messages retrieved
   */   
  public static List<Message> get(String queueUrl) {
    // long poll the queue for 20 seconds to read up to 5 messages
    return get(queueUrl, 5, 20);    
  }
  
  /**
   * Retrieve up to the specified number of messages from the queue provided.
   * The queue will be polled up to 20 seconds.
   * All message attributes will be retrieved.
   * 
   * @param queueUrl Target queue's URL
   * @param maxNumberOfMessages maximum number of messages to be retrieved
   * @return The list of messages retrieved
   */   
  public static List<Message> get(String queueUrl, int maxNumberOfMessages) {
    // long poll the queue for 20 seconds to read up to <maxNumberOfMessages> messages
    return get(queueUrl, maxNumberOfMessages, 20);
  }
  
  /**
   * Retrieve up to the specified number of messages from the queue provided
   * The queue will be polled up to the specified amount of time.
   * All message attributes will be retrieved.
   * 
   * @param queueUrl Target queue's URL
   * @param maxNumberOfMessages maximum number of messages to be retrieved
   * @param waitTimeSeconds maximum number of seconds to wait
   * @return The list of messages retrieved
   */  
  public static List<Message> get(String queueUrl, int maxNumberOfMessages, int waitTimeSeconds) {
    SqsClient sqsClient = buildClient();
    
    // long poll the queue for <waitTimeSeconds> seconds to read up to <maxNumberOfMessages> messages
    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
      .queueUrl(queueUrl)
      .maxNumberOfMessages(maxNumberOfMessages)
      .waitTimeSeconds(waitTimeSeconds)
      .attributeNames(QueueAttributeName.ALL)
      .build();
    
    List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

    // delete from the queue all messages read
    for (Message message : messages) {
      DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
        .queueUrl(queueUrl)
        .receiptHandle(message.receiptHandle())
        .build();
      sqsClient.deleteMessage(deleteMessageRequest);
    }
    
    return messages;
  }
}