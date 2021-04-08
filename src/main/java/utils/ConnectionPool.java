package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Properties;

public class ConnectionPool {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    private static LinkedList<Connection> connectionList;

    public static Connection getConnection() {

        if (connectionList == null) {
            synchronized (ConnectionPool.class) {
                if (connectionList == null) {
                    connectionList = new LinkedList();
                    createConnection();

                }
            }
        }

        return connectionList.poll();
    }

    private static void createConnection() {
        Properties properties = GlobalConfig.instance("dev", "conf").properties;
        String driver = properties.getProperty("driver","com.mysql.jdbc.Driver");
        String url = properties.getProperty("url", "jdbc:mysql://192.168.1.1:3306/test");
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        int poolSize = Integer.valueOf(properties.getProperty("pool.size", "1"));


        System.out.println(properties.toString());
        try {
            Class.forName(driver);
            for (int i = 0; i < poolSize; i++) {
                Connection connection = DriverManager.getConnection(url, user, password);
                connectionList.push(connection);
                logger.info("success create {} size db connection");
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeCollection();
        }
    }

    public static void closeCollection() {
        if (connectionList != null) {
            synchronized (ConnectionPool.class) {
                if (connectionList != null) {
                    // 关闭所有连接
                    connectionList.forEach(connection -> {
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
                    connectionList = null;
                }
            }
        }

        logger.info("close all db connection success");
    }

    public static void returnConntion(Connection connection) {
        connectionList.push(connection);
    }


}