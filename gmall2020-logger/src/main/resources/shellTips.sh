bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic gmall_start_0523 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic gmall_event_0523 --partitions 3 --replication-factor 1

bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic gmall_start_0523

运行 kafka 消费者，准备消费数据（因为日活需要启动日志，所以我们这里只测试启动日志）
bin/kafka-console-consumer.sh --bootstrap-server hadoop202:9092 --topic gmall_start_bak
➢ 运行采集数据的 jar
[atguigu@hadoop202 rt_gmall]$ java -jar gmall2020-logger-0.0.1-SNAPSHOT.jar
➢ 运行模拟生成数据的 jar
[atguigu@hadoop202 rt_applog]$ java -jar gmall2020-mock-log-2020-05-10.jar