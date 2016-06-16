package dkvs.sockets;

import dkvs.InputMessage;
import dkvs.OutputMessage;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by amir.
 */
public class SocketHandler {
    private final Thread writer;
    private final Thread listener;
    private final BlockingQueue<OutputMessage> outputQueue;
    private final Socket socket;
    private final AtomicLong lastResponse;
    private final AtomicInteger serverNum;
    private boolean stopped;

    SocketHandler(Socket socket, int i, ServerSocketListener mainListener) {
        stopped = false;
        lastResponse = new AtomicLong(System.currentTimeMillis());
        this.socket = socket;
        this.serverNum = new AtomicInteger(i);
        outputQueue = new LinkedBlockingQueue<>();
        writer = new Thread(() -> {
            try (Writer writer = new PrintWriter(socket.getOutputStream())) {
                while (true) {
                    try {
                        if (Thread.interrupted()) break;
                        OutputMessage message = outputQueue.take();
                        System.out.println(String.format("sending message from %d to %d: %s",
                                mainListener.replicaNumber, serverNum.get(), message.getText()));
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
                    lastResponse.set(System.currentTimeMillis());
                    System.out.println(String.format("receiving message from %d to %d: %s",
                            serverNum.get(), mainListener.replicaNumber, request));
                    if (request.startsWith("node ")) {
                        int node = Integer.valueOf(request.substring(5));
                        this.serverNum.set(node);
                        mainListener.serverList[node] = this;
                        mainListener.clientsList.remove(this);
                        continue;
                    }
                    if (request.equals("PONG")) continue;
                    if (request.equals("PING")) {
                        outputQueue.add(new OutputMessage("PONG"));
                        continue;
                    }
                    mainListener.inputQueue.add(new InputMessage(request, this));
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
        if (!stopped) {
            stopped = true;
            System.out.println("Stopping socket");
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("Can't close socket(((");
            }
            writer.interrupt();
        }
    }

    long getLastResponse() {
        return lastResponse.get();
    }

    public int getServerNum() {
        return serverNum.get();
    }
}
