package utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class C3p0Util {



    public static void main(String[] args) throws SQLException {

        ComboPooledDataSource dataSource = new ComboPooledDataSource();

        dataSource.setJdbcUrl("jdbc:mysql://192.168.1.210:3306/test?useSSL=false");
        dataSource.setUser("hive");
        dataSource.setPassword("hive");

        Connection connection = dataSource.getConnection();
        String sql = "insert into t1(name, age) values ('lisi1', 30)";
        System.out.println("sql is "+ sql);

        PreparedStatement pstat = connection.prepareStatement(sql);
        pstat.execute();

        pstat.close();
        connection.close();
        System.out.println("execute over");


    }
}
