environments {
    // 开发环境
    dev {
        // kafka
        bootstrap.servers = "localhost:9092"
        group.id = {
            String id ->
                return "study_"+ id
        }

        // redis
        host = "localhost"
        port = 6379

        // mysql
        driver = "com.mysql.jdbc.Driver"
        url = "jdbc:mysql://192.168.1.1:3306/test?useSSL=false"
        user = "root"
        password = "123456"
        pool.size = 5

    }

    // 测试环境
    test {}

    // 生产环境
    proc {}
}