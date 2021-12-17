package com.atguigu.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 订单service接口
 */
@Service
public interface ClickHouseService {
    public BigDecimal getOrderAmount(String date);
    public Map<String,BigDecimal> getOrderAmountHour(String date);
}
