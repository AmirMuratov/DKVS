import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by amir.
 */
public class KeyValueStorage {
    private Map<String, String> storage;
    private OperationLogger logger;

    public KeyValueStorage(int id) {
        storage = new HashMap<>();
        logger = new OperationLogger(id);
        logger.getHistory().forEach(x -> x.applyOperation(storage));
    }

    public String get(String key) {
        return storage.get(key);
    }
    public void delete(String key) {
        Operation op = Operation.getDeleteOperation(key);
        op.applyOperation(storage);
        logger.addOperation(op);
    }
    public void set(String key, String value) {
        Operation op = Operation.getSetOperation(key, value);
        op.applyOperation(storage);
        logger.addOperation(op);

    }
}
