import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestJdbc {

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver"); // 加载驱动

        Connection connection = DriverManager.getConnection(
                "jdbc:mysql://47.110.230.144:8306/analysis?useUnicode=true&characterEncoding=UTF-8&useSSL=false",
                "sbxu_f",
                "sbxu@1o2o");

        PreparedStatement preparedStatement = connection.prepareStatement("select count(*) as cnt from person");
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            int cnt = resultSet.getInt("cnt");

            System.out.println("total cnt is "+  cnt);
        }


        if (resultSet != null) {
            resultSet.close();
        }

        if (preparedStatement != null) {
            preparedStatement.close();
        }

        if (connection != null) {
            connection.close();
        }

    }
}
