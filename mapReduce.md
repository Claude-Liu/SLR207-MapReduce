# MapReduce Project

In this file we present the following aspects.

1. How to build it
2. Some details about the code (thread, how to synchronize different sockets)
3. Experiment results (the timing of each phase, the maximum size of the dataset that I acheive to compute)
4. Potential optimizations could be done
5. What would happen if a node crashes

Because of the time and bad time organization, I miss the part of the discussion of Amdahl's law



## how to build

1. The folder structure.

   ```
   mapreduce/
   ├── myftpclient/
   │   ├── src/
   │   │   └── main/
   │   │       └── java/
   |	|			└── rs/
   │   │           	└── MyClient.java
   │   ├── target/
   │   │   ├── archive-tmp/
   │   │   ├── classes/
   │   │   ├── generated-sources/
   │   │   ├── maven-status/
   │   │   └── myftpclient-1-jar-with-dependencies.jar
   │   ├── pom.xml
   │   └── README.md
   ├── myftpserver/
   │   ├── src/
   │   │   └── main/
   │   │       ├── java/
   |	|			└── rs/
   |	|				└── MyServer.java
   │   │       └── resources/
   │   ├── target/
   │   │   ├── archive-tmp/
   │   │   ├── classes/
   │   │   ├── generated-sources/
   │   │   ├── maven-status/
   │   │   └── myserver-1-jar-with-dependencies.jar
   │   ├── pom.xml
   │   ├── README.md
   │   └── users.properties
   ├── mapReduce.md
   ```

2. compile the server and the client.

   ```bash
   cd ./myftpclient
   mvn clean compile assembly:single
   cd ../myftpserver
   mvn clean compile assembly:single
   ```

3. send the jar file to remote machines. **(I did not provide the interface to change machine name or use more than 3 machines. Please execute exactly the following command)**

   ```bash
   // open four terminals and login to different machines
   ssh lliu-22@tp-3b07-04 //(client N0)
   ssh lliu-22@tp-3b07-03 //(server N3)
   ssh lliu-22@tp-3b07-02 //(server N2)
   ssh lliu-22@tp-3b07-01 //(server N1)
   
   // create the work dir in everymachine
   cd /tmp
   mkdir linfeng
   
   // send the myftpclient jar to remote machines
   //go to the local machine
   cd ./myftpclient
   scp ./target/myftpclient-1-jar-with-dependencies.jar lliu-22@tp-3b07-04:/tmp/linfeng
   
   // send the myserver jar to remote machines
   cd ../myftpserver
   scp ./target/myserver-1-jar-with-dependencies.jar lliu-22@tp-3b07-03:/tmp/linfeng
   scp ./target/myserver-1-jar-with-dependencies.jar lliu-22@tp-3b07-02:/tmp/linfeng
   scp ./target/myserver-1-jar-with-dependencies.jar lliu-22@tp-3b07-01:/tmp/linfeng
   
   ```

4. start the servers and the client to test our MapReduce algorithm.

   ```bash
   // start the server on different machines
   ssh lliu-22@tp-3b07-03 //(server N3)
   ssh lliu-22@tp-3b07-02 //(server N2)
   ssh lliu-22@tp-3b07-01 //(server N1)
   
   cd /tmp/linfeng
   java -jar myserver-1-jar-with-dependencies.jar
   
   // start the client and test the default file
   ssh lliu-22@tp-3b07-04 //(client N0)
   cd /tmp/linfeng
   java -jar myftpclient-1-jar-with-dependencies.jar
   or
   // start the client and choose a filename from /cal/commoncrawl
   ssh lliu-22@tp-3b07-04 //(client N0)
   cd /tmp/linfeng
   java -jar myftpclient-1-jar-with-dependencies.jar CC-MAIN-20230321002050-20230321032050-00472.warc.wet
   ```
   
   

## code details

We use thread in three places

1. We use client in the class of MySocketClient. In fact, we have three socket client in the master node to communicate with three computation server. With three threads, these sockets can run concurrently.

   ```java
   class MySocketClient extends Thread
       
   // in main function of master node
   MySocketClient socketClient1 = new MySocketClient(server[0], socketPort, num_ReadOK_countdown, num_Shuffle1OK_countdown, num_Reduce1OK_countdown, num_Shuffle2OK_countdown, sharedArrayMax, sharedArrayMin, true);
   MySocketClient socketClient2 = new MySocketClient(server[1], socketPort, num_ReadOK_countdown, num_Shuffle1OK_countdown, num_Reduce1OK_countdown, num_Shuffle2OK_countdown, sharedArrayMax, sharedArrayMin, false);
   MySocketClient socketClient3 = new MySocketClient(server[2], socketPort, num_ReadOK_countdown, num_Shuffle1OK_countdown, num_Reduce1OK_countdown, num_Shuffle2OK_countdown, sharedArrayMax, sharedArrayMin, false);
   socketClient1.start();
   socketClient2.start();
   socketClient3.start();
   ```

2. In the computation server, both the SocketServer and FTPServer act as service providers. They must remain active, continuously listening for incoming requests from clients.

   To handle multiple client connections efficiently, we use the `ClientHandler` class, which implements the `Runnable` interface. This design allows the server to manage concurrent client interactions. Each time a new client connects, a new thread is spawned from the thread pool to handle the client's requests, ensuring scalable and responsive service management.

   ```java
   class FTPServerThread extends Thread {
       public void run ( ) {
           FTPServer ftpServer = new FTPServer();
           ftpServer.startServerHandler();
       }
   }
   ```

   ```java
   class SocketServer extends Thread{
       public void run() {
           ExecutorService executor = Executors.newFixedThreadPool(10); // Create a thread pool
           try (ServerSocket serverSocket = new ServerSocket(7228)) {
               System.out.println("Server is running and waiting for clients...");
   
               while (true) {
                   Socket clientSocket = serverSocket.accept(); // Accept a client connection
                   System.out.println("Client connected: " + clientSocket);
   
                   // Create a new thread to handle the client
                   Runnable clientHandler = new ClientHandler(clientSocket);
                   executor.execute(clientHandler);
                   ...
               }
           } 
           ...
   }
       
   class ClientHandler implements Runnable
   ```

   

## experiment results

In this section we show the the timing of each phase.

The two phase map reduce can be divided into the following phrases

1. Split : The client node split the big data file into three small files with similar line numbers, and send the three small files to different server node to count their word frequency.
2. READ_FILE: This is the map phase in the first round mapreduce. Each server node count the word frequency in their own file and record them in a map in Java.
3. SHUFFLE1: This is the shuffle phase in the first round mapreduce. Each server split their words into three classes according to their hash code. They keep the words belong to themselves and send the other words to corresponding nodes using FTP protocol.
4. REDUCE1: This is the reduce phase in the first round mapreduce. Each server count the frequency of their own words, gathering the files from other nodes.
5. SHUFFLE2: The second shuffle is based on the word frequency. We make sure that the three server nodes have words with frequency in growing order.
6. REDUCED: Each node gather the files sent by other nodes in SHUFFLE2.



We can find that the split time and map time in first mapreduce are dominant. The split time is from the communication cost between client and server. The map time in the first mapreduce is from the traversal in the big data and getting word counts.

The map time can be reduced with more computing nodes and the split time is can not be greatly reduced because the communication bandwidth of the client node is limited. Generally, we speculate that more the overall time can be decreased with more node. It is a pity that we did not verify this through experiments.

<img src="D:\IE\SLR\SLR207\mapreduce_time_distribution.png" alt="mapreduce_time_distribution" style="zoom:67%;" />



