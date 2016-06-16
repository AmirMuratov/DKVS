import dkvs.Main;

import java.io.File;
import java.io.IOException;

/**
 * Created by amir.
 */
public class ClearLogs {
    private static final String FILENAME = "logs/dksv_%d.log";
    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 100; i++) {
            File f = new File(String.format(FILENAME, i));
            if  (f.exists()) {
                f.delete();
            }
        }
    }
}
