package com.atguigu.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 从 ads 层中获取数据提供的服务接口
 */
@Service
public interface MySQLService {
    public List<Map> getTrademardStat(String startDate, String endDate, int topN);
}
