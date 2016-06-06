package dkvs;

import dkvs.kvsservice.KeyValueStorage;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class PrimaryServer implements Runnable {
    private int replicaNumber;
    private BackupServer.ServerStatus status;
    private LinkedBlockingQueue<Message> queue;
    private int curOperationNumber;
    private KeyValueStorage service;

    public PrimaryServer(int replicaNumber, LinkedBlockingQueue<Message> queue) {
        this.replicaNumber = replicaNumber;
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            Message curMessage;
            try {
                curMessage = queue.take();
            } catch (InterruptedException e) {
                //
                return;
            }
            switch (curMessage.type) {
                case REQUEST:

                    break;
                //case
            }
        }
    }
}
