package utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DbUtil {

    public static void insert(String sql) {
        Connection connection = ConnectionPool.getConnection();

        PreparedStatement pstat = null;
        try {
            pstat = connection.prepareStatement(sql);
            pstat.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pstat != null) {
                try {
                    pstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
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


    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {

                int age = new Random().nextInt(50);
                String name = "zhangsan_"+ age;
                String sql = String.format(" insert into test(name, age) values ('?', ?) ", name, age);

                DbUtil.insert(sql);

            }).start();
        }

    }

}

