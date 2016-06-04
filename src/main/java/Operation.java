import java.util.HashMap;
import java.util.Map;

/**
 * Created by amir.
 */
public class Operation {
    private OperationType type;
    private String key;
    private String value;
    public static Operation getDeleteOperation(String key) {
        Operation op = new Operation();
        op.type = OperationType.DELETE;
        op.key = key;
        return op;
    }
    public static Operation getSetOperation(String key, String value) {
        Operation op = new Operation();
        op.type = OperationType.SET;
        op.key = key;
        op.value = value;
        return op;
    }
    private Operation() {
    }

    public OperationType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void applyOperation(Map<String, String> map) {
        if (type.equals(OperationType.SET)) {
            map.put(key, value);
        }
        if (type.equals(OperationType.DELETE)) {
            map.remove(key);
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(type.toString()).append(" ").append(key);
        if (type.equals(OperationType.SET)) {
            result.append(" ").append(value);
        }
        result.append("\n");
        return result.toString();
    }
}
