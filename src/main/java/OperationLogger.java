import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.omg.PortableServer.ServantRetentionPolicy;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by amir.
 */
public class OperationLogger {
    private List<Operation> log;
    private Writer writer;
    private static final String FILENAME = "logs/dksv_%d.log";

    public OperationLogger(int id) {
        log = new ArrayList<>();
        File logFile = new File(String.format(FILENAME, id));
        if (logFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line = reader.readLine();
                while (line != null) {
                    String[] parts = line.split(" ");
                    log.add(parts[0].equals("DELETE") ? Operation.getDeleteOperation(parts[1])
                            : Operation.getSetOperation(parts[1], parts[2]));
                    line = reader.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addOperation(Operation op) {
        log.add(op);
        try {
            writer.write(op.toString());
            writer.flush();
        } catch (IOException e) {
            System.err.println("Can't write to file");
        }
    }

    public List<Operation> getHistory() {
        return log;
    }

}
