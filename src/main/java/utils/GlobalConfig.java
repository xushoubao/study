package utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class GlobalConfig {

    private static GlobalConfig config = new GlobalConfig();
    Properties properties = new Properties();

    private GlobalConfig() {
        try {
            FileInputStream fis = new FileInputStream(new File("conf/study.properties"));
            properties.load(fis);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static GlobalConfig instance() {
        return config;
    }


    public static void main(String[] args) {
        Object groupId = GlobalConfig.instance().properties.get("bootstrap.servers");

        System.out.println(groupId);

    }
}
