environments {
    // 开发环境
    dev {
        // kafka
        bootstrap.servers = "localhost:9092"
        group.id = "test"

        // redis
        host = "localhost"
        port = 6379

        // mysql
        input.hostname = "localhost";
        input.port = 3306
        input.user = "root"
        input.password = "123456"
        input.databaseList = "business"  // 多个用,分割
        input.tableList = "business.person"; //如果不写则监控库下的所有表，需要使用，多个用,分割【库名.表名】

        output.driver = "com.mysql.jdbc.Driver"
        output.url = "jdbc:mysql://localhost:3306/warehouse?useUnicode=true&characterEncoding=UTF-8&useSSL=false"
        output.user = "root"
        output.password = "123456"
        output.database = "warehouse"
        output.table = "person"

        // 连接池大小
        pool.size = 5
        // 连接池里面没有可用连接时，尝试的次数
        retry.count = 5
        // 每次重新申请连接间隔的时间
        sleep.second.time = 2

    }

    com {
        bootstrap.servers = "47.111.76.193:9093,114.215.183.42:9093,47.99.173.167:9093"
        group.id = "test"
        // mysql
        input.hostname = "47.110.230.144";
        input.port = 8306
        input.user = "myzhu_f"
        input.password = "myzhu@1o2o"
        input.databaseList = "content_config"  // 多个用,分割
        input.tableList = "content_config.person"; //如果不写则监控库下的所有表，需要使用，多个用,分割【库名.表名】

        output.driver = "com.mysql.jdbc.Driver"
        output.url = "jdbc:mysql://47.110.230.144:8306/content_config?useUnicode=true&characterEncoding=UTF-8&useSSL=false"
        output.user = "myzhu_f"
        output.password = "myzhu@1o2o"
        output.database = "content_config"
        output.table = "person_new"

    }

    // 测试环境
    test {}

    // 生产环境
    proc {}
}