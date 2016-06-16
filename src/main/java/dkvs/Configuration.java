package dkvs;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by amir.
 */
public class Configuration {
    private static final String PROPERTY_FILE = "dkvs.properties";
    private static volatile int timeout;
    private static volatile int numberOfServers;
    private static volatile Map<Integer, Integer> numberToPortMap = new ConcurrentHashMap<>();
    private static volatile Map<Integer, String> numberToIPMap = new ConcurrentHashMap<>();

    static {
        numberToPortMap = new HashMap<>();
        numberToIPMap = new HashMap<>();
        try (FileInputStream inputStream = new FileInputStream(PROPERTY_FILE)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            numberOfServers = properties.size() - 1;
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                if (key.equals("timeout")) {
                    timeout = Integer.valueOf(value);
                } else {
                    numberToIPMap.put(Integer.valueOf(key.substring(5)), value.split(":")[0]);
                    numberToPortMap.put(Integer.valueOf(key.substring(5)), Integer.valueOf(value.split(":")[1]));
                }
                //System.out.println(key + " => " + value);
            }
        } catch (IOException e) {
            System.err.println("Can't load " + PROPERTY_FILE + " file");
        }
    }

    public static int getNumberOfServers() {
        return numberOfServers;
    }

    public static int getTimeout() {
        return timeout;
    }

    public static int getPort(int serverNumber) {
        return numberToPortMap.get(serverNumber);
    }

    public static String getIP(int serverNumber) {
        return numberToIPMap.get(serverNumber);
    }
}
