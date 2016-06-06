package dkvs;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by amir.
 */
public class Configuration {
    private static final String PROPERTY_FILE = "dkvs.properties";
    private static int timeout;
    private static int numberOfservers;
    private static Map<String, Integer> addressMap = new HashMap<>();
    private static Map<Integer, Integer> numberToPortMap = new HashMap<>();
    private static Map<Integer, String> numberToIPMap = new HashMap<>();
    static {
        try (FileInputStream inputStream = new FileInputStream(PROPERTY_FILE)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            numberOfservers = properties.size() - 1;
            for(String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                if (key.equals("timeout")) {
                    timeout = Integer.valueOf(value);
                } else {
                    numberToIPMap.put(Integer.valueOf(key.substring(5)), value.split(":")[0]);
                    numberToPortMap.put(Integer.valueOf(key.substring(5)), Integer.valueOf(value.split(":")[1]));
                    addressMap.put("/" + value, Integer.valueOf(key.substring(5)));
                }
                System.out.println(key + " => " + value);
            }
        } catch (IOException e) {
            System.err.println("Can't load " + PROPERTY_FILE + " file");
        }
    }

    public static int getNumberOfservers() {
        return numberOfservers;
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
    public static int getServerNumber(String address) {
        if (!addressMap.containsKey(address)) {
            return -1;
        }
        return addressMap.get(address);
    }

}
