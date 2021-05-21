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

    com {
        // mysql
        input.hostname = "47.110.230.144";
        input.port = 8306
        input.user = "mpo"
        input.password = "mponline"
        input.databaseList = "analysis"  // 多个用,分割
        input.tableList = "analysis.person"; //如果不写则监控库下的所有表，需要使用，多个用,分割【库名.表名】

        output.driver = "com.mysql.jdbc.Driver"
        output.url = "jdbc:mysql://47.110.230.144:8306/warehouse?useUnicode=true&characterEncoding=UTF-8&useSSL=false"
        output.user = "mpo"
        output.password = "mponline"


    }

    // 测试环境
    test {}

    // 生产环境
    proc {}
}