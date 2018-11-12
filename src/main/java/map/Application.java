
// Denis test-map
// server command line: java -jar build/libs/heatmap-1.0.0.jar <Lab: 1,2,both. Default: both> <Vehicles. Defaul: 10> <Vehicles real refresh interval in sec. Default: 60> 
// example: java -jar build/libs/heatmap-1.0.0.jar both 10 2
// client command line: localhost:8080

package map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Bean;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import software.amazon.awssdk.services.sqs.model.Message;
import qm.QueueManager;

import static java.util.stream.Collectors.*;
import java.util.concurrent.BlockingQueue;
import java.util.*;

@EnableWebSocket
@SpringBootApplication
public class Application {
  
    private static final String HEATMAP_QUEUE_URL = "https://sqs.eu-west-2.amazonaws.com/556385395922/heatmap-queue";
    private static final String HEATMAP_SUPPLIER_URL = "https://sqs.eu-west-2.amazonaws.com/556385395922/heatmap-supplier";      
    private static final String DEFAULT_LOCATION_FILE = "./files/realtimelocation.csv";
    private static String lab;
    private static String[] args;
    
    // Map of supplier's messages queue per client
    private static final Map<String, BlockingQueue<String>> clients = new HashMap<>();

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        
        // start a thread to handle lab 1 messages received
        new Thread(() -> {
          List<Message> msgs;
          
          System.out.println("Heatspots will be read from the queue: " + HEATMAP_QUEUE_URL);
          while(true){
            // Poll the queue waiting for coordinates
            msgs = QueueManager.get(HEATMAP_QUEUE_URL); 
            if (msgs.isEmpty()) {
              continue;
            }
            // Get messages received and concatenate them
            String lines = msgs.stream()
            .map(Message::body)
//            .peek(System.out::println)
            .collect(joining("\n"));
            
            // Put the message into the clients' queues
            for (Map.Entry<String, BlockingQueue<String>> client : clients.entrySet()){
              if (!client.getValue().offer(lines)){
                System.out.println(client.getKey() + " client's queue is full. Received message discarded!");
              }
            }
          }
        }).start();
    }
    
    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
          
          final String DEFAULT_LAB = "both";
    
          this.args = args;
          lab = ((args.length > 0) ? args[0] : DEFAULT_LAB);
          switch(lab) {
            case "1":
              System.out.println("Lab 1 selected");
              break;
            case "2":
              System.out.println("Lab 2 selected");
              break;
            default:
              lab = DEFAULT_LAB; 
              System.out.println("Labs 1 and 2 selected");
          }

        };
    }
    
    @Component
    public static class MyWebSocketConfigurer implements WebSocketConfigurer {

        @Override
        public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
            registry.addHandler(new MyBinaryHandler(), "/lab");
        }
    }
    
    @Component
    public static class MyBinaryHandler extends BinaryWebSocketHandler {

      public void afterConnectionEstablished(WebSocketSession session) {
        if (lab.equals("1") || lab.equals("both")) {
          try {
            // Send start command to supplier
            QueueManager.put(HEATMAP_SUPPLIER_URL, "start");
            
            new Lab1(session, clients);
          }
          catch( Exception e) {
            System.out.println("Exception cought while trying to write into the queue: " + HEATMAP_SUPPLIER_URL);
            e.printStackTrace();
          }          
        }
        if (lab.equals("2") || lab.equals("both")) {
          new Lab2(session, args, DEFAULT_LOCATION_FILE);
        }  
      }
    }
}