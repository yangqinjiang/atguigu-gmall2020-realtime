- 启动 Zookeeper
- 启动 Kafka
- 启动 logger.sh（ 日志处理服务-Nginx 和 SpringBoot 程序）
- 启动 Redis， 清空 Redis 中所有数据 127.0.0.1:6379> FLUSHALL
- Idea 中运行程序
  - 运行模拟生成日志的 jar
    PUT _template/gmall2020_dau_info_template
    {
    "index_patterns": ["gmall2020_dau_info*"],
    "settings": {
    "number_of_shards": 3
    },
    "aliases": {
    "{index}-query": {},
    "gmall2020_dau_info-query": {}
    },
    "mappings": {
    "_doc": {
    "properties": {
    "mid": {
    "type": "keyword"
    },
    "uid": {
    "type": "keyword"
    },
    "ar": {
    "type": "keyword"
    },
    "ch": {
    "type": "keyword"
    },
    "vc": {
    "type": "keyword"
    },
    "dt": {
    "type": "keyword"
    },
    "hr": {
    "type": "keyword"
    },
    "mi": {
    "type": "keyword"
    },
    "ts": {
    "type": "date"
    }
    }
    }
    }
    }
