package com.atguigu.gmall.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 订单service接口
 */
public interface ClickHouseService {
    public BigDecimal getOrderAmount(String date);
    public Map<String,BigDecimal> getOrderAmountHour(String date);
}
