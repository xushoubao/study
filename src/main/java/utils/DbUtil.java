package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DbUtil {

    private static Logger logger = LoggerFactory.getLogger(DbUtil.class);

    public static void insert(String sql) {
        Connection connection = null;
        while ((connection = ConnectionPool.getConnection()) == null) {
            try {
                Thread.sleep(1000L);
                logger.warn("connection is null, try again after {} seconds", 1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        PreparedStatement pstat = null;
        try {
            pstat = connection.prepareStatement(sql);
            logger.warn("sql [{}] use conn [{}], pstat [{}]", sql, connection.hashCode(), pstat.hashCode());
            pstat.execute();
            logger.info("sql [{}] execute success", sql);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pstat != null) {
                try {
                    pstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    ConnectionPool.returnConntion(connection);
                    pstat = null;
                }
            }
        }

    }

    public static void delete() {

    }

    public static List<Object> query() {

        return new ArrayList<>();
    }

    public static void update() {

    }


    public static void testInsert() {
        ConnectionPool.start();
        try {

            for (int i = 0; i < 10; i++) {
                new Thread(() -> {

                    int age = new Random().nextInt(50);
                    String name = "zhangsan_"+ age;
                    String sql = String.format(" insert into test(name, age) values ('%s', %s) ", name, age);

                    DbUtil.insert(sql);

                }).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

//            ConnectionPool.closeCollection();
        }
    }


    public static void main(String[] args) {
        testInsert();
    }

}

