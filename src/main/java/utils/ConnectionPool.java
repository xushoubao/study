package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class ConnectionPool {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    static ArrayBlockingQueue<Connection> queue = null;

    static Properties properties = null;

    /**
     * 启动连接池
     */
    public static void start() {
        init();
    }

    /**
     * 关闭连接池
     */
    public static void stop(){
        closeCollection();
    }

    /**
     * 申请一个连接
     * @return
     * @throws Exception
     */
    public static Connection applyConnection() throws Exception {
        Connection connection = null;

        int retryCount = (int) properties.getOrDefault("retry.count", 10);
        int sleepSecondTime = (int) properties.getOrDefault("sleep.second.time", 1);

        while ((connection = queue.poll()) == null && retryCount-- > 0 ) {
            try {
                Thread.sleep(sleepSecondTime * 1000L);
                logger.debug("connection is null, try again after {} seconds, still retry {} times, current thread {}",
                        sleepSecondTime, retryCount, Thread.currentThread().getName());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (connection == null) {
            throw new Exception("get connection failed, please increase poos.size or retry.count or sleep.second.time");
        }

        return connection;
    }

    /**
     * 归还一个连接
     * @param connection
     */
    public static void returnConntion(Connection connection) {
        if (connection != null) {
            queue.offer(connection);
        }
    }

    /**
     * 初始化配置和线程池
     * @return
     */
    private static void init() {
        if (queue == null) {
            synchronized (ConnectionPool.class) {
                if (queue == null) {
                    queue = new ArrayBlockingQueue(10);
                    initConfig();
                    initConnection();
                }
            }
        }
    }

    /**
     * 加载配置
     */
    private static void initConfig() {
        properties = GlobalConfig.instance("dev", "conf").properties;
    }

    /**
     * 创建连接，并初始化连接池
     */
    private static void initConnection() {
        String driver = properties.getProperty("driver","com.mysql.jdbc.Driver");
        String url = properties.getProperty("url");
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        int poolSize = (int) properties.getOrDefault("pool.size", 1);

        try {
            Class.forName(driver);
            for (int i = 0; i < poolSize; i++) {
                Connection connection = DriverManager.getConnection(url, user, password);
                queue.offer(connection);
                logger.debug("success create {} size db connection", i+1);
            }
            logger.info("create all db connection success");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            closeCollection();
        } finally {

        }
    }

    /**
     * 关闭连接，并清空连接池
     */
    private static void closeCollection() {
        if (queue != null) {
            synchronized (ConnectionPool.class) {
                if (queue != null) {
                    // 关闭所有连接
                    queue.forEach(connection -> {
                        if (connection != null) {
                            try {
                                connection.close();
                                queue.remove(connection);
                                logger.debug("still {} connectios in connection pool", queue.size());
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {

                            }
                        }
                    });
                    // 将连接池置为空
                    queue = null;
                    logger.info("close all db connection success");
                }
            }
        }
    }

}