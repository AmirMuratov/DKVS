package dkvs;

import dkvs.sockets.ServerSocketListener;

import java.io.Console;


public class Main {

    public static void start(int replicaNumber, boolean primary) {
        ServerSocketListener listener = new ServerSocketListener(Configuration.getPort(replicaNumber), replicaNumber);
        listener.start();
        if (primary) {
            PrimaryServer server = new PrimaryServer(replicaNumber, listener);
            new Thread(server).start();
        } else {
            BackupServer server = new BackupServer(replicaNumber, listener);
            new Thread(server).start();
        }
    }

    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            start(Integer.valueOf(args[0]), false);
        } else {
            for (int i = 1; i <= Configuration.getNumberOfServers(); i++) {
                start(i, i == 1);
            }
        }
    }
}
