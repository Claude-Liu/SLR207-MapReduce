package rs;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;
import java.net.*;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.log4j.PropertyConfigurator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



class FTPServer {
    public void startServerHandler ( ) {
        PropertyConfigurator.configure(MyServer.class.getResource("/log4J.properties"));
        FtpServerFactory serverFactory = new FtpServerFactory();
        int port = 7227; // Replace 3456 with the desired port number

        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setPort(port);

        serverFactory.addListener("default", listenerFactory.createListener());

        // Create a UserManager instance
        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        File userFile = new File("users.properties");
        if (!userFile.exists()) {
            try {
                if (userFile.createNewFile()) {
                    System.out.println("File created: " + userFile.getName());
                } else {
                    System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
        
        userManagerFactory.setFile(userFile); // Specify the file to store user details
        userManagerFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor()); // Store plain text passwords
        UserManager userManager = userManagerFactory.createUserManager();
        // Create a user
        BaseUser user = new BaseUser();
        user.setName("linfeng"); // Replace "username" with the desired username
        user.setPassword("0926"); // Replace "password" with the desired password
        String username = user.getName();
        String homeDirectory = System.getProperty("java.io.tmpdir")  + "/" + username; 
        File directory = new File(homeDirectory); // Convert the string to a File object
        if (!directory.exists()) { // Check if the directory exists
            if (directory.mkdirs()) {
                System.out.println("Directory created: " + directory.getAbsolutePath());
            } else {
                System.out.println("Failed to create directory.");
            }
        }
        user.setHomeDirectory(homeDirectory);
        // Set write permissions for the user
        List<Authority> authorities = new ArrayList<>();
        authorities.add(new WritePermission());
        user.setAuthorities(authorities);
        user.setHomeDirectory(homeDirectory);

        // Add the user to the user manager
        try {
            userManager.save(user);
        } catch (FtpException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // Set the user manager on the server context
        serverFactory.setUserManager(userManager);

        FtpServer server = serverFactory.createServer();

        // start the server
        try {
            server.start();
            System.out.println("FTP Server started on port " + port);
            
        } catch (FtpException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

class MyFTPClient {

    String server;
    int port;
    String username;
    String password;

    public void sendFile(String localFilePath, String remoteFilename) {
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(server, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            try (InputStream inputStream = new FileInputStream(localFilePath)) {
                System.out.println("Start uploading file: " + localFilePath);
                boolean done = ftpClient.storeFile(remoteFilename, inputStream);
                if (done) {
                    System.out.println("File uploaded successfully to " + remoteFilename);
                } else {
                    System.out.println("Could not upload the file.");
                }
            }

            ftpClient.logout();
            ftpClient.disconnect();
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    public MyFTPClient(String server, int port, String username, String password) {
        this.server = server;
        this.port = port;
        this.username = username;
        this.password = password;
    }
}

class FTPServerThread extends Thread {
    public void run ( ) {
        FTPServer ftpServer = new FTPServer();
        ftpServer.startServerHandler();
    }
}

// class SocketServer 
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
                //serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown(); // Shutdown the thread pool gracefully when done
        }
    }
}

class ClientHandler implements Runnable {
    private Map<String, Integer> wordCounts = new HashMap<>();
    private Map<String, Integer> ReducedWordCounts = new HashMap<>();
    private Map<String, Integer> finalWordCounts = new HashMap<>();
    private MySocketClient socketClient1;
    private MySocketClient socketClient2;
    private MyFTPClient ftpClient1;
    private MyFTPClient ftpClient2;
    private int serverId;
    int socketPort = 7228;

    private Socket clientSocket;
    public int getMaxValue() {
        if (ReducedWordCounts.isEmpty()) {
            throw new IllegalStateException("Map is empty, no max value.");
        }
        return Collections.max(ReducedWordCounts.values());
    }

    public int getMinValue() {
        if (ReducedWordCounts.isEmpty()) {
            throw new IllegalStateException("Map is empty, no min value.");
        }
        return Collections.min(ReducedWordCounts.values());
    }

    public static int[] extractPartition(String input) {
        String regex = "SHUFFLE2_PARTITIONS\\[(\\d+)\\]\\[(\\d+)\\]";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            int part1 = Integer.parseInt(matcher.group(1));
            int part2 = Integer.parseInt(matcher.group(2));
            return new int[]{part1, part2};
        } else {
            return null;
        }
    }
    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }
    
    public void sendMessage(String message, BufferedWriter out) {
        try {
            out.write(message);
            out.newLine();
            out.flush();
            System.out.println("Message sent: " + message + " to client");
        }
        catch (Exception e) {
            System.err.println("Error sending message");
        }
    }
    

    public String receiveMessage(BufferedReader in) {
        String response = null;
        try {
            System.out.println("Waiting for message from client");
            while ((response = in.readLine()) == null) {
                //System.out.println("No message received, waiting...");
                Thread.sleep(10);
            }
            System.out.println("Received response: " + response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return response;
    }


    public void readFile(String path) throws IOException {
        Path filePath = Paths.get(path);
        if (!Files.exists(filePath)) {
            System.out.println("No such directory: " + path);
            return;
        }

        Files.walkFileTree(filePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                System.out.println("File: " + file);
                // skip if file not end with .wet
                if (!file.toString().endsWith(".wet")) {
                    return FileVisitResult.CONTINUE;
                }
                splitWords(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public void splitWords(Path file) {
        try {
            BufferedReader reader = Files.newBufferedReader(file);
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.toLowerCase();
                List<String> words = MultiLanguageWordSegmenter.segmentWords(line);
                for (String word : words) {
                    if (wordCounts.containsKey(word)) {
                        wordCounts.put(word, wordCounts.get(word) + 1);
                    } else {
                        wordCounts.put(word, 1);
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            System.err.println("error happens when reading file: " + e.getMessage());
        }
    }

    public void createSocketClient() {
        if (serverId==1){
            this.socketClient1 = new MySocketClient("tp-3b07-02", socketPort);
            this.socketClient2 = new MySocketClient("tp-3b07-03", socketPort);
        }
        else if(serverId==2){
            this.socketClient1 = new MySocketClient("tp-3b07-01", socketPort);
            this.socketClient2 = new MySocketClient("tp-3b07-03", socketPort);
        }
        else if(serverId==3){
            this.socketClient1 = new MySocketClient("tp-3b07-01", socketPort);
            this.socketClient2 = new MySocketClient("tp-3b07-02", socketPort);
        }
        else{
            System.out.println("Invalid server number");
        }
    }

    public void createFTPClient() {
        if (serverId==1){
            this.ftpClient1 = new MyFTPClient("tp-3b07-02", 7227, "linfeng", "0926");
            this.ftpClient2 = new MyFTPClient("tp-3b07-03", 7227, "linfeng", "0926");
        }
        else if(serverId==2){
            this.ftpClient1 = new MyFTPClient("tp-3b07-01", 7227, "linfeng", "0926");
            this.ftpClient2 = new MyFTPClient("tp-3b07-03", 7227, "linfeng", "0926");
        }
        else if(serverId==3){
            this.ftpClient1 = new MyFTPClient("tp-3b07-01", 7227, "linfeng", "0926");
            this.ftpClient2 = new MyFTPClient("tp-3b07-02", 7227, "linfeng", "0926");
        }
        else{
            System.out.println("Invalid server number");
        }
    }

    public void hashShuffle() {
        try (BufferedWriter writer0 = new BufferedWriter(new FileWriter("hash0.txt"));
             BufferedWriter writer1 = new BufferedWriter(new FileWriter("hash1.txt"));
             BufferedWriter writer2 = new BufferedWriter(new FileWriter("hash2.txt"))) {

            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();
                int hash = word.hashCode();
                int bucket = Math.abs(hash) % 3;

                String line = word + "\t\t" + count + "\n";
                
                switch (bucket) {
                    case 0:
                        writer0.write(line);
                        break;
                    case 1:
                        writer1.write(line);
                        break;
                    case 2:
                        writer2.write(line);
                        break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void partitionShuffle(int part1, int part2) {
        try (
            BufferedWriter writer1 = new BufferedWriter(new FileWriter("part0.txt"));
            BufferedWriter writer2 = new BufferedWriter(new FileWriter("part1.txt"));
            BufferedWriter writer3 = new BufferedWriter(new FileWriter("part2.txt"))
        ) {
            for (Map.Entry<String, Integer> entry : ReducedWordCounts.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();
                String line = word + "\t\t" + count + "\n";
                
                if (count < part1) {
                    writer1.write(line);
                } else if (count <= part2) {
                    writer2.write(line);
                } else {
                    writer3.write(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void distributeFTPFiles(String fileType) {
        // send the files to the servers
        // use the first socket client and ftp client
        
        String hashFilePrefix = "hash";
        String partitionFilePrefix = "part";

        String filePrefix;
        if ("hashShuffle".equals(fileType)) {
            filePrefix = hashFilePrefix;
        } else if ("partitionShuffle".equals(fileType)) {
            filePrefix = partitionFilePrefix;
        } else {
            throw new IllegalArgumentException("Invalid file type");
        }

        if (serverId == 1) {
            ftpClient1.sendFile(filePrefix + "1.txt", filePrefix + "1_1.txt");
        } else if (serverId == 2) {
            ftpClient1.sendFile(filePrefix + "0.txt", filePrefix + "0_2.txt");
        } else if (serverId == 3) {
            ftpClient1.sendFile(filePrefix + "0.txt", filePrefix + "0_3.txt");
        } else {
            System.out.println("Invalid server number");
        }
        socketClient1.sendMessage("FTP_SHUFFLE");
        socketClient1.receiveMessage();
    
        if (serverId == 1) {
            ftpClient2.sendFile(filePrefix + "2.txt", filePrefix + "2_1.txt");
        } else if (serverId == 2) {
            ftpClient2.sendFile(filePrefix + "2.txt", filePrefix + "2_2.txt");
        } else if (serverId == 3) {
            ftpClient2.sendFile(filePrefix + "1.txt", filePrefix + "1_3.txt");
        } else {
            System.out.println("Invalid server number");
        }
        socketClient2.sendMessage("FTP_SHUFFLE");
        socketClient2.receiveMessage();
    }

    public void merge(String[] fileNames, String shuffleNo) {

        for (String fileName : fileNames) {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t\t");
                    if (parts.length == 2) {
                        String word = parts[0].trim();
                        int count = Integer.parseInt(parts[1].trim());
                        if (shuffleNo.equals("hashShuffle")) {
                            ReducedWordCounts.put(word, ReducedWordCounts.getOrDefault(word, 0) + count);
                        }
                        else if (shuffleNo.equals("partitionShuffle")) {
                            finalWordCounts.put(word, finalWordCounts.getOrDefault(word, 0) + count);
                        }
                        else{
                            System.out.println("Invalid shuffle number");
                        
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void reduce(String shuffleNo){
        String hashFilePrefix = "hash";
        String partitionFilePrefix = "part";
        String filePrefix;
        if ("hashShuffle".equals(shuffleNo)) {
            filePrefix = hashFilePrefix;
        } else if ("partitionShuffle".equals(shuffleNo)) {
            filePrefix = partitionFilePrefix;
        } else {
            throw new IllegalArgumentException("Invalid file type");
        }

        String[] fileNames = new String[3];
        if (serverId == 1) {
            fileNames[0] = filePrefix + "0.txt";
            fileNames[1] = filePrefix + "0_2.txt";
            fileNames[2] = filePrefix + "0_3.txt";
        } else if (serverId == 2) {
            fileNames[0] = filePrefix + "1.txt";
            fileNames[1] = filePrefix + "1_1.txt";
            fileNames[2] = filePrefix + "1_3.txt";
        } else if (serverId == 3) {
            fileNames[0] = filePrefix + "2.txt";
            fileNames[1] = filePrefix + "2_1.txt";
            fileNames[2] = filePrefix + "2_2.txt";
        }
        else{
            System.out.println("Invalid server number");
        }
        merge(fileNames,  shuffleNo);
        if ("hashShuffle".equals(shuffleNo)) {
            writeWordCounts("reduced_hash.txt", ReducedWordCounts);
        } else if ("partitionShuffle".equals(shuffleNo)) {
            writeWordCounts("results.txt", finalWordCounts);
        } else {
            throw new IllegalArgumentException("Invalid file type");
        }
    }

    public void writeWordCounts(String fileName, Map<String, Integer> wordCounts) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();
                writer.write(word + "\t\t" + count + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        try(
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
        )
        {  
            while(true){
                String clientMessage = receiveMessage(in);
                System.out.println("Received message from client: " + clientMessage);
                if (clientMessage.startsWith("READ_FILE")){
                    // get server number
                    char secondLastChar = clientMessage.charAt(clientMessage.length() - 2);
                    if (Character.isDigit(secondLastChar)) {
                        this.serverId = Character.getNumericValue(secondLastChar);
                        // print serverId
                        System.out.println("Server ID: " + serverId);
                        // create socket
                        createSocketClient();
                        System.out.println("Socket client created");
                        createFTPClient();
                        System.out.println("FTP client created");
                    } else {
                        throw new NumberFormatException("The second last character is not a digit.");
                    }
                    // read file and get word count
                    readFile("/tmp/linfeng/data");
                    //print the word count
                    //for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                    //    System.out.println(entry.getKey() + ": " + entry.getValue());
                    //}
                    //response with ok
                    sendMessage("READ_OK", out);
                }
                else if(clientMessage.equals("SHUFFLE1")){
                    // hash shuffle
                    hashShuffle();
                    distributeFTPFiles("hashShuffle");
                    sendMessage("SHUFFLE1_OK", out);
                }
                else if (clientMessage.equals("REDUCE_1")){
                    reduce("hashShuffle");
                    int max = getMaxValue();
                    int min = getMinValue();
                    sendMessage(String.format("REDUCE_1_OK_MAX[%d]_MIN[%d]",max, min), out);
                }
                else if (clientMessage.equals("FTP_SHUFFLE")){
                    // receive the file
                    // to do
                    sendMessage("FTP_OK", out);
                }
                else if (clientMessage.startsWith("SHUFFLE2")){
                    int[] partitions = extractPartition(clientMessage);
                    if (partitions != null) {
                        int part1 = partitions[0];
                        int part2 = partitions[1];
                        partitionShuffle(part1, part2);
                        distributeFTPFiles("partitionShuffle");
                        sendMessage("SHUFFLE2_OK", out);
                    } else {
                        sendMessage("SHUFFLE_2_ERROR", out);
                    }
                }
                else if(clientMessage.equals("REDUCE_2")){
                    reduce("partitionShuffle");
                    sendMessage("REDUCE_2_OK", out);
                }
                else{
                    System.out.println("Invalid message from client");
                }
            }
        }
        catch (IOException e) {
            //e.printStackTrace();
        } 
        finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                //e.printStackTrace();
            }
        }
    }
}

class MySocketClient{   
    // this class is for communicating with the servers and synchronizing the servers during mapreduce

    public String server;
    public int socketPort;
    private Socket clientSocket;
    BufferedWriter out = null;
    BufferedReader in = null;

    public MySocketClient(String server, int socketPort) {
        this.server = server;
        this.socketPort = socketPort;
        try {
            clientSocket = new Socket(server, socketPort);
            // Create output stream at the client (to send data to the server)
            out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            //out.println(serverName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String message) {
        try {
            out.write(message);
            out.newLine();
            out.flush();
            System.out.println("Message sent: " + message + " to server " + server);
        }
        catch (Exception e) {
            System.err.println("Error sending message");
        }
    }

    public String receiveMessage() {
        try {
            System.out.println("Waiting for message from client");
            String response = in.readLine();
            System.out.println("Received response: " + response);
            return response;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}

class MultiLanguageWordSegmenter {

    public static List<String> segmentWords(String text) {
        List<String> words = new ArrayList<>();
        StringBuilder word = new StringBuilder();

        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);

            if (Character.isWhitespace(ch)) {
                addWord(words, word);
            } else if (isCJKCharacter(ch)) {
                addWord(words, word);
                words.add(String.valueOf(ch));
            } else if (isWordCharacter(ch)) {
                word.append(ch);
            } else {
                addWord(words, word);
                words.add(String.valueOf(ch));
            }
        }

        addWord(words, word);

        return words;
    }

    private static void addWord(List<String> words, StringBuilder word) {
        if (word.length() > 0) {
            words.add(word.toString());
            word.setLength(0);
        }
    }

    public static boolean isWordCharacter(char ch) {
        return Character.isLetterOrDigit(ch) || ch == '\'';
    }

    public static boolean isCJKCharacter(char ch) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(ch);
        return block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS ||
               block == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS ||
               block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A ||
               block == Character.UnicodeBlock.HIRAGANA ||
               block == Character.UnicodeBlock.KATAKANA ||
               block == Character.UnicodeBlock.HANGUL_SYLLABLES;
    }
}


public class MyServer {

    public static void main(String[] args) {
        String file_dir = "./data";
        File file = new File(file_dir);
        if (!file.exists()) {
            if (file.mkdirs()) {
                System.out.println("Directory is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }
        FTPServerThread ftpServerThread = new FTPServerThread();
        ftpServerThread.start();
        SocketServer socketServer = new SocketServer();
        socketServer.start();
    }
}
