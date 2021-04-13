package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class DbUtil {

    private static Logger logger = LoggerFactory.getLogger(DbUtil.class);

    /**
     * 执行一条sql，增删改
     * @param sql
     * @throws Exception
     */
    public static void execute(String sql) throws Exception {
        Connection connection = ConnectionPool.applyConnection();

        PreparedStatement pstat = null;
        try {
            pstat = connection.prepareStatement(sql);
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



    public static List<Object> query() {

        return new ArrayList<>();
    }


    public static void testexecute() throws Exception {
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);

        ConnectionPool.start();
        try {

            for (int i = 0; i < count; i++) {
                new Thread(() -> {

                    int age = new Random().nextInt(50);
                    String name = "zhangsan_"+ age;
                    String sql = String.format(" insert into test(name, age) values ('%s', %s) ", name, age);

                    try {
                        DbUtil.execute(sql);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    latch.countDown();

                }).start();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (latch.getCount() != 0) {
            latch.await();
        }
        ConnectionPool.stop();
    }


    public static void main(String[] args) throws Exception {
        testexecute();
    }

}

