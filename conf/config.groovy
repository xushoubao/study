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

        // 连接池大小
        pool.size = 5
        // 连接池里面没有可用连接时，尝试的次数
        retry.count = 5
        // 每次重新申请连接间隔的时间
        sleep.second.time = 2

    }

    // 测试环境
    test {}

    // 生产环境
    proc {}
}