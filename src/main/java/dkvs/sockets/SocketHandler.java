package dkvs.sockets;

import dkvs.InputMessage;
import dkvs.OutputMessage;

import java.io.*;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by amir.
 */
public class SocketHandler {
    private final Thread writer;
    private final Thread listener;
    private final LinkedBlockingQueue<OutputMessage> outputQueue;
    private final Socket socket;
    private final AtomicLong lastResponse;
    private final AtomicInteger serverNum;


    public SocketHandler(Socket socket, Queue<InputMessage> inputQueue, int serverNum, SocketHandler[] serverList) {
        lastResponse = new AtomicLong(System.currentTimeMillis());
        this.socket = socket;
        this.serverNum = new AtomicInteger(serverNum);
        outputQueue = new LinkedBlockingQueue<>();
        writer = new Thread(() -> {
            try (Writer writer = new PrintWriter(socket.getOutputStream())) {
                while (true) {
                    try {
                        if (Thread.interrupted()) break;
                        OutputMessage message = outputQueue.take();
                        writer.write(message.getText() + '\n');
                        writer.flush();
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } catch (IOException e) {
                System.err.println("Can't create socket writer");
            }
        });
        listener = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                while (true) {
                    String request = reader.readLine();
                    if (request == null) {
                        throw new IOException();
                    }
                    if (request.startsWith("node ")) {
                        int node = Integer.valueOf(request.substring(5));
                        this.serverNum.set(node);
                        serverList[node] = this;
                        continue;
                    }
                    System.out.println("new Message: " + request);
                    lastResponse.set(System.currentTimeMillis());
                    inputQueue.add(new InputMessage(request, this));
                }
            } catch (IOException e) {
                stop();
            }
        });
        start();//START
    }

    public void sendMessage(OutputMessage message) {
        outputQueue.add(message);
    }

    public void start() {
        writer.start();
        listener.start();
    }

    public void stop() {
        System.out.println("Stopping " + serverNum + " socket");
        try {
            socket.close();
        } catch (IOException e) {
            System.err.println("Can't close socket(((");
        }
        writer.interrupt();
        //TODO interrupt
        listener.interrupt();
    }

    public long getLastResponse() {
        return lastResponse.get();
    }

    public int getServerNum() {
        return serverNum.get();
    }
}
