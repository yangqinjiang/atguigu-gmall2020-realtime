package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.mapper.OrderWideMapper;
import com.atguigu.gmall.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 订单service接口实现
 */
@Service
public class ClickHouseServiceImpl implements ClickHouseService {
    @Autowired
    OrderWideMapper orderWideMapper;
    @Override
    public BigDecimal getOrderAmount(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        List<Map> mapList = orderWideMapper.selectOrderAmountHourMap(date);
        //等待返回的数据变量
        HashMap<String, BigDecimal> orderAmountHourMap = new HashMap<>();
        for (Map map : mapList) {
            //注意： 0-9 小时取出后需要在前面补 0
            orderAmountHourMap.put(String.format("%02d",map.get("hr")),(BigDecimal) map.get("am"));
        }
        return orderAmountHourMap;
    }
}
