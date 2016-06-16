import dkvs.Configuration;
import dkvs.Main;
import dkvs.sockets.ServerSocketListener;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by amir.
 */
public class Tester {
    public static void main(String[] args) throws IOException {
        /*for (int i = 1; i <= 9; i++) {
            String[] s = {"" + i};
            new Thread(() -> {
                Main.main(s);
            }).start();
        }*/
        //Main.start(1, false);
        //Main.start(2, false);
        //Main.start(3, false);
        /*new Thread(() -> {
            ServerSocket socket = null;
            try {
                socket = new ServerSocket(1488);
                Socket socket1 = socket.accept();
                while (true) {
                    socket1.getInputStream().
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
*/
    }
}
