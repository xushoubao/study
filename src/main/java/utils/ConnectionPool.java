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

    public static void start() {
        getConnection();
    }

    public static Connection getConnection() {

        if (queue == null) {
            synchronized (ConnectionPool.class) {
                if (queue == null) {
                    queue = new ArrayBlockingQueue(10);
                    createConnection();

                }
            }
        }

        return queue.poll();
    }

    private static void createConnection() {
        Properties properties = GlobalConfig.instance("dev", "conf").properties;
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
                logger.info("success create {} size db connection, connection hashcode is {}", i+1, connection.hashCode());
            }

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

    public static void closeCollection() {
        if (queue != null) {
            synchronized (ConnectionPool.class) {
                if (queue != null) {
                    // 关闭所有连接
                    queue.forEach(connection -> {
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                connection = null;
                            }
                        }
                    });

                    // 将连接池置为空
                    queue = null;
                }
            }
        }

        logger.info("close all db connection success");
    }

    public static void returnConntion(Connection connection) {
        if (connection != null) {
            queue.offer(connection);
        }
    }


}