package dkvs.sockets;

import dkvs.Message;

import java.io.*;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class SocketHandler {
    private final Thread writer;
    private final Thread listener;
    private final LinkedBlockingQueue<Message> outputQueue;
    private final Socket socket;

    public SocketHandler(Socket socket, Queue<Message> inputQueue) {
        this.socket = socket;
        outputQueue = new LinkedBlockingQueue<>();
        writer = new Thread(() -> {
            try (Writer writer = new PrintWriter(socket.getOutputStream())) {
                while (true) {
                    try {
                        if (Thread.interrupted()) break;
                        Message message = outputQueue.take();
                        writer.write(message.toString() + '\n');
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
                    inputQueue.add(new Message(request, this));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        start();//START
    }

    public void sendMessage(Message message) {
        outputQueue.add(message);
    }

    public Queue<Message> getOutputQueue() {
        return outputQueue;
    }

    public void start() {
        writer.start();
        listener.start();
    }

    public void stop() {
        writer.interrupt();
        //TODO interrupt
        listener.interrupt();
        try {
            socket.close();
        } catch (IOException e) {
            System.err.println("Can't close socket(((");
        }
    }

}
