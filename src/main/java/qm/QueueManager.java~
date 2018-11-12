package qm;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.*;

public class QueueManager {
  
  private static final String TEST_QUEUE_URL = "https://sqs.eu-west-2.amazonaws.com/556385395922/test-sqs";

  public static void main(String[] args) {
    String TEST_MSG = "Test message number: " + System.currentTimeMillis();

    System.out.println("Testing QueueManager class using queue: " + TEST_QUEUE_URL);
    System.out.println("The following message will be written into the queue...");
    System.out.println(TEST_MSG);
    
    try {
      put(TEST_QUEUE_URL, TEST_MSG);
    }
    catch( Exception e) {
      System.out.println("Exception cought while trying to write into the queue...");
      e.printStackTrace();
      System.exit(1);
    }
    
    System.out.println("Press ENTER...");
    Scanner sc = new Scanner(System.in);
    sc.nextLine();
    
    System.out.println("The following messages have been read from the queue...");
    get(TEST_QUEUE_URL) 
      .stream()
      .map(msg -> msg.body())
      .forEach(System.out::println);
  }
  
  private static SqsClient buildClient() {
    // Build a SQS service client
    return SqsClient.builder()
      .region(Region.EU_WEST_2)
      .credentialsProvider(ProfileCredentialsProvider.builder()
                             .profileName("default")
                             .build())
      .build();      
  }
  public static String put(String queueUrl, String messageBody) throws Exception{
    return buildClient().sendMessage(SendMessageRequest.builder()
                                   .queueUrl(queueUrl)
                                   .messageBody(messageBody)
                                   .build())
      .messageId();    
  }
  
  
  public static String put(String queueUrl, String messageBody, Integer delaySeconds) throws Exception{ 
    return buildClient().sendMessage(SendMessageRequest.builder()
                                   .queueUrl(queueUrl)
                                   .messageBody(messageBody)
                                   .delaySeconds(delaySeconds)
                                   .build())
      .messageId();
  }
  
  public static List<Message> get(String queueUrl) {
    // long poll the queue for 20 seconds to read up to 5 messages
    return get(queueUrl, 5, 20);    
  }
  
  public static List<Message> get(String queueUrl, int maxNumberOfMessages) {
    // long poll the queue for 20 seconds to read up to <maxNumberOfMessages> messages
    return get(queueUrl, maxNumberOfMessages, 20);
  }
  
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