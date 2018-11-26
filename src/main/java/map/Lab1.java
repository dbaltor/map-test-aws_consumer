package map;

import static java.util.stream.Collectors.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.*;

import org.springframework.web.socket.WebSocketSession;

class Lab1 extends Thread {
  WebSocketSession session;
  Map<String, BlockingQueue<String>> clients;
  
  private static final int MAX_PENDING_MSG = 6;
  
  public Lab1(WebSocketSession session, Map<String, BlockingQueue<String>> clients) {
    super();
    this.session = session;
    this.clients = clients;
    this.start();
  }
  
  public void run() {
    //***************** DEBUG
    //System.out.println("NEW CLIENT on the list!!! " + session.getId());
    //*****************
    // add this client's queue to the Map clients
    clients.put(session.getId(), new ArrayBlockingQueue<>(MAX_PENDING_MSG));
    String lines = "";
    while (true) {
      // Poll the client queue waiting for a message
      try {
        lines = clients.get(session.getId()).take();
      } catch(InterruptedException ie) {
        ie.printStackTrace();
        continue;
      }
      // Split lines and send them one by one
      for(String line: lines.split("\n")){
        if (!WsPacket.send(session, "m1," + line)){
          //***************** DEBUG
          //System.out.println("CLIENT LEAVING from the list!!! " + session.getId());
          //*****************          
          // Client socket is closed. Remove client's queue from the Map clients and exit!
          clients.remove(session.getId());
          System.out.println("Lab 1 finished. Socket " + session.getId() + " closed!");
          return; 
        }
      }            
    }    
  }
}
