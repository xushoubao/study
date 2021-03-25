environments {
    // 开发环境
    dev {
        bootstrap.servers = "localhost:9092"
        group.id = {
            String id ->
                return "study_"+ id
        }

    }

    // 测试环境
    test {}

    // 生产环境
    proc {}
}