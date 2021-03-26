package utils;

import groovy.util.ConfigSlurper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// 单例，提供不同的实例方式，如果在同一个程序中使用，可以使用reInstance对配置进行重载
public class GlobalConfig {

    private Logger logger = LoggerFactory.getLogger(GlobalConfig.class);

    private volatile static GlobalConfig globalConfig;

    public Map<String, Object> groovy = new HashMap<>();

    public Properties properties = new Properties();

    private ConfigSlurper configSlurper;

    private GlobalConfig() {
    }

    private void initGroovy(String groovyEnv, File groovyFile) {
        if (configSlurper == null && StringUtils.isNotEmpty(groovyEnv)) {
            configSlurper = new ConfigSlurper(groovyEnv);
        }

        try {
            Map map = configSlurper.parse(groovyFile.toURI().toURL()).flatten();
            groovy.putAll(map);
        } catch (Exception e) {
            logger.error("init groovy failed: {}",e);
            e.printStackTrace();
        }
    }

    private void initProperties(File propertiesFile) {
        try {
            properties.load(new FileInputStream(propertiesFile));

        } catch (Exception e) {
            logger.error("init properties failed: {}", e);
            e.printStackTrace();
        }
    }

    public static GlobalConfig instance(String groovyEnv, String configDir) {
        return instance(groovyEnv, configDir, null);
    }

    public static GlobalConfig instance(String groovyEnv, String configDir, FileFilter filter) {
        // double check
        if (globalConfig == null) {
            synchronized (GlobalConfig.class) {
                if (globalConfig == null) {
                    globalConfig = new GlobalConfig();

                    globalConfig. loadConfig(groovyEnv, configDir, filter);
                }
            }
        }
        return globalConfig;
    }

    public static GlobalConfig reInstance(String groovyEnv, String configDir) {
        return reInstance(groovyEnv, configDir, null);
    }

    // 重新加载配置，可以按需进行配置重载，以及配置热加载
    public static GlobalConfig reInstance(String groovyEnv, String configDir, FileFilter filter) {
        if (globalConfig != null) {
            synchronized (GlobalConfig.class) {
                if (globalConfig != null) {
                    globalConfig.properties.clear();
                    globalConfig.groovy.clear();
                    globalConfig = null;
                }
            }
        }

        return instance(groovyEnv, configDir, filter);
    }

    //加载配置文件
    private void loadConfig(String groovyEnv, String configDir, FileFilter filter) {

        if (configDir != null) {
            File configDirFile = new File(configDir);
            if (!configDirFile.isDirectory()) {
                logger.warn("congif dir is not directory");
            }

            // 加载新的配置
            for (File file : configDirFile.listFiles(filter)) {
                if (file.getName().endsWith(".properties")) {
                    globalConfig.initProperties(file);
                } else if (file.getName().endsWith(".groovy")) {
                    globalConfig.initGroovy(groovyEnv, file);
                }
            }
        }
    }

    public static void test() {
        GlobalConfig config = GlobalConfig.instance("dev","conf");

//        String bootstrapServers = (String) config.groovy.get("bootstrap.servers");
//        System.out.println("bootstrap.server="+ bootstrapServers);
//
//        Object groupId = ((Closure) config.groovy.get("group.id")).call("test");
//        System.out.println("group.id="+ groupId);
//
//        String study = (String) config.properties.get("study");
//        System.out.println("study="+ study);

        System.out.println("config :"+ config.properties);
        System.out.println("config :"+ config.groovy);

        System.out.println("===============");

        GlobalConfig filterConfig = GlobalConfig.reInstance("dev", "conf",
                pathname -> pathname.getName().equals("test.groovy"));

        System.out.println("filterConfig:"+ filterConfig.properties);
        System.out.println("filterConfig:"+ filterConfig.groovy);

    }

    public static void main(String[] args) {
        test();
    }
}
