package utils;

import groovy.lang.Closure;
import groovy.util.ConfigObject;
import groovy.util.ConfigSlurper;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class GlobalGroovyConfig {

    private static GlobalGroovyConfig globalGroovyConfig = new GlobalGroovyConfig();

    public Map<String, Object> config = new HashMap<>();

    private GlobalGroovyConfig() {

        ConfigObject parse = null;
        try {
            parse = new ConfigSlurper("dev").parse(new File("conf/config.groovy").toURI().toURL());
        } catch (Exception e) {
            e.printStackTrace();
        }
        config = parse.flatten();
    }

    public static GlobalGroovyConfig instance() {
        return globalGroovyConfig;
    }

    public static void main(String[] args) {
        Object value = GlobalGroovyConfig.instance().config.get("bootstrap.servers");
        System.out.println("bootstrap.server="+ value);

        String groupId =(String) ((Closure) GlobalGroovyConfig.instance().config.get("group.id")).call("test");
        System.out.println("group.id="+ groupId);
    }
}
