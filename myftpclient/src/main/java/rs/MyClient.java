package rs;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

class MySocketClient extends Thread{
    private final CountDownLatch num_ReadOK_countdown;   
    private final CountDownLatch num_Shuffle1OK_countdown;
    private final CountDownLatch num_Reduce1OK_countdown;
    private final CountDownLatch num_Shuffle2OK_countdown;
    // this class is for communicating with the servers and synchronizing the servers during mapreduce

    public String server;
    private int serverId;
    private final AtomicIntegerArray sharedArrayMax;
    private final AtomicIntegerArray sharedArrayMin;
    public int socketPort;
    private Socket clientSocket;
    private boolean isTimer;
    BufferedWriter out = null;
    BufferedReader in = null;

    public MySocketClient(String server, int socketPort, 
                        CountDownLatch num_ReadOK_countdown, CountDownLatch num_Shuffle1OK_countdown, 
                        CountDownLatch num_Reduce1OK_countdown, CountDownLatch num_Shuffle2OK_countdown, 
                        AtomicIntegerArray sharedArrayMax, AtomicIntegerArray sharedArrayMin,
                        boolean isTimer) {
        this.server = server;
        this.socketPort = socketPort;
        this.num_ReadOK_countdown = num_ReadOK_countdown;
        this.num_Shuffle1OK_countdown = num_Shuffle1OK_countdown;
        this.num_Reduce1OK_countdown = num_Reduce1OK_countdown;
        this.num_Shuffle2OK_countdown = num_Shuffle2OK_countdown;
        this.sharedArrayMax = sharedArrayMax;
        this.sharedArrayMin = sharedArrayMin;
        this.isTimer = isTimer;
        try {
            clientSocket = new Socket(server, socketPort);
            setServerId();
            // Create output stream at the client (to send data to the server)
            out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            //out.println(serverName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setServerId(){
        char secondLastChar = server.charAt(server.length() - 1);
        if (Character.isDigit(secondLastChar)) {
            this.serverId = Character.getNumericValue(secondLastChar);
            System.out.println("--------------Server ID: " + serverId + "----------------");
        }
        else{
            throw new IllegalArgumentException("Server name is not in the correct format.");
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

    public String recieveMessage() {
        try {
            String response = in.readLine();
            System.out.println("Received response: " + response + "from server" + server);
            return response;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static int[] extractIntegers(String input) {
        String regex = "REDUCE_1_OK_MAX\\[(\\d+)\\]_MIN\\[(\\d+)\\]";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            int max = Integer.parseInt(matcher.group(1));
            int min = Integer.parseInt(matcher.group(2));
            return new int[]{max, min};
        } else {
            return null;
        }
    }

    public void waitRead () {
        this.num_ReadOK_countdown.countDown();
        //System.out.println("Server " + server + " has read the file.");
        try {
            this.num_ReadOK_countdown.await();
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void waitShuffle1 ( ) {
        num_Shuffle1OK_countdown.countDown();
        try {
            num_Shuffle1OK_countdown.await();
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void waitReduced1 ( ) {
        num_Reduce1OK_countdown.countDown();
        try {
            num_Reduce1OK_countdown.await();
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void waitShuffle2(){
        num_Shuffle2OK_countdown.countDown();
        try {
            num_Shuffle2OK_countdown.await();
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public static int findMax(AtomicIntegerArray array) {
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < array.length(); i++) {
            int value = array.get(i);
            if (value > max) {
                max = value;
            }
        }
        return max;
    }
    public static int findMin(AtomicIntegerArray array) {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < array.length(); i++) {
            int value = array.get(i);
            if (value < min) {
                min = value;
            }
        }
        return min;
    }

    private int[] zipfThreePartitions(int minFreq, int maxFreq) {
        double logMinFreq = Math.log(minFreq);
        double logMaxFreq = Math.log(maxFreq);

        double logT1 = logMinFreq + (logMaxFreq - logMinFreq) / 3;
        double logT2 = logMinFreq + 2 * (logMaxFreq - logMinFreq) / 3;

        int t1 = (int) Math.exp(logT1);
        int t2 = (int) Math.exp(logT2);

        return new int[]{t1, t2};
    }

    public void run() {
        try {
            long startTime = System.currentTimeMillis();
            this.sendMessage("READ_FILE" + "[" + server + "]");
            this.recieveMessage();
            waitRead();
            long endTime1 = System.currentTimeMillis();
            if (isTimer) {
                System.out.println("-------- READ_FILE time (ms): " + (endTime1 - startTime));
            }
            System.out.println("All servers have read the file.");

            this.sendMessage("SHUFFLE1");
            this.recieveMessage();
            waitShuffle1();
            long endTime2 = System.currentTimeMillis();
            if (isTimer) {
                System.out.println("-------- SHUFFLE1 time (ms): " + (endTime2 - endTime1));
            }
            System.out.println("All servers have shuffle the file.");

            this.sendMessage("REDUCE_1");
            String messageMaxMin = recieveMessage();
            int[] maxMin = extractIntegers(messageMaxMin);
            sharedArrayMax.set(serverId-1, maxMin[0]);
            sharedArrayMin.set(serverId-1, maxMin[1]);
            System.out.println("local Max: " + maxMin[0] + "local Min: " + maxMin[1]);
            waitReduced1();
            long endTime3 = System.currentTimeMillis();
            if (isTimer) {
                System.out.println("-------- REDUCE1 time (ms): " + (endTime3 - endTime2));
            }
            int overallMax = findMax(sharedArrayMax);
            int overallMin = findMin(sharedArrayMin);
            System.out.println("Overall Max: " + overallMax + " Overall Min: " + overallMin);
            System.out.println("All servers have reduced the file.");

            int[] partitions = zipfThreePartitions(overallMin, overallMax);
            this.sendMessage("SHUFFLE2_PARTITIONS[" + partitions[0] + "][" + partitions[1] + "]");
            this.recieveMessage();
            waitShuffle2();
            long endTime4 = System.currentTimeMillis();
            if (isTimer) {
                System.out.println("-------- SHUFFLE2 time (ms): " + (endTime4 - endTime3));
            }
            System.out.println("All servers have shuffled the file.");

            this.sendMessage("REDUCE_2");
            this.recieveMessage();
            long endTime5 = System.currentTimeMillis();
            if (isTimer) {
                System.out.println("-------- REDUCE2 time(ms): " + (endTime5 - endTime4));
            }
            clientSocket.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}

public class MyClient {
    public static void splitFile(String inputFilePath, String outputFilePath1, String outputFilePath2, String outputFilePath3) throws IOException {
        List<String> lines = readFile(inputFilePath);

        int totalLines = lines.size();
        int partSize = totalLines / 3;
        int remainder = totalLines % 3;

        writeFile(outputFilePath1, lines, 0, partSize + (remainder > 0 ? 1 : 0));
        writeFile(outputFilePath2, lines, partSize + (remainder > 0 ? 1 : 0), 2 * partSize + (remainder > 1 ? 1 : 0));
        writeFile(outputFilePath3, lines, 2 * partSize + (remainder > 1 ? 1 : 0), totalLines);
    }

    private static List<String> readFile(String filePath) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    private static void writeFile(String filePath, List<String> lines, int start, int end) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (int i = start; i < end; i++) {
                writer.write(lines.get(i));
                writer.newLine();
            }
        }
    }
    public static void main(String[] args) {

        String testFileName = "CC-MAIN-20230321002050-20230321032050-00472.warc.wet";
        if (args.length == 1) {
            testFileName = args[0];
        }
        else if (args.length == 0){
            System.out.println("No input file name provided. Using default file name: " + testFileName);
        }
        else{
            System.out.println("Too many arguments. Please provide only one input file name.");
            return;
        }

        String[] server = {"tp-3b07-01", "tp-3b07-02", "tp-3b07-03"};
        String file_dir = "/tmp/linfeng/data/";
        // create folder if not exist
        File dir = new File(file_dir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String filename = "/cal/commoncrawl/" + testFileName;
        String[] filenames = {file_dir + "bigData_1.wet", file_dir + "bigData_2.wet", file_dir + "bigData_3.wet"};
        long startTime = System.currentTimeMillis();
        try{
            splitFile(filename, filenames[0], filenames[1], filenames[2]);
        }
        catch(Exception e){System.out.println("Error in splitting file.");}
        
        String[] remoteFilenames = {"data/test1.wet", "data/test2.wet", "data/test3.wet"};
        String username = "linfeng";
        String password = "0926";
        int FTPPort = 7227;
        int socketPort = 7228;

        AtomicIntegerArray sharedArrayMax = new AtomicIntegerArray(3);
        AtomicIntegerArray sharedArrayMin = new AtomicIntegerArray(3);

        CountDownLatch num_ReadOK_countdown = new CountDownLatch(3);
        CountDownLatch num_Shuffle1OK_countdown = new CountDownLatch(3);
        CountDownLatch num_Reduce1OK_countdown = new CountDownLatch(3);
        CountDownLatch num_Shuffle2OK_countdown = new CountDownLatch(3);

        MyFTPClient myFTPServer1 = new MyFTPClient(server[0], FTPPort, username, password);
        MyFTPClient myFTPServer2 = new MyFTPClient(server[1], FTPPort, username, password);
        MyFTPClient myFTPServer3 = new MyFTPClient(server[2], FTPPort, username, password);
        
        myFTPServer1.sendFile(filenames[0], remoteFilenames[0]);
        myFTPServer2.sendFile(filenames[1], remoteFilenames[1]);
        myFTPServer3.sendFile(filenames[2], remoteFilenames[2]);
        long endTime = System.currentTimeMillis();
        System.out.println("-------- Split Time (ms)" + (endTime - startTime));

        System.out.println("FTP client finished.");
        MySocketClient socketClient1 = new MySocketClient(server[0], socketPort, num_ReadOK_countdown, num_Shuffle1OK_countdown, num_Reduce1OK_countdown, num_Shuffle2OK_countdown, sharedArrayMax, sharedArrayMin, true);
        MySocketClient socketClient2 = new MySocketClient(server[1], socketPort, num_ReadOK_countdown, num_Shuffle1OK_countdown, num_Reduce1OK_countdown, num_Shuffle2OK_countdown, sharedArrayMax, sharedArrayMin, false);
        MySocketClient socketClient3 = new MySocketClient(server[2], socketPort, num_ReadOK_countdown, num_Shuffle1OK_countdown, num_Reduce1OK_countdown, num_Shuffle2OK_countdown, sharedArrayMax, sharedArrayMin, false);
        socketClient1.start();
        socketClient2.start();
        socketClient3.start();

        try{socketClient1.join();} catch(Exception e){}
        try{socketClient2.join();} catch(Exception e){}
        try{socketClient3.join();} catch(Exception e){}
        
    }
}
