package com.atguigu.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public interface ESService {
    //日活的总数查询
    public Long getDauTotal(String date);
    //新增设备
    public Long getNewMidTotal(String date);
    //日活的分时查询
    public Map getDauHour(String date);
}
