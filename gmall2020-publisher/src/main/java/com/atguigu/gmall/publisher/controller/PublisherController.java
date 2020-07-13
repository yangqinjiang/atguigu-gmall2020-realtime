package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    ESService esService;

    @RequestMapping("/hello")
    public String hello() {
        return "hello world";
    }

    @GetMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt) {
        ArrayList<Map<String, Object>> rsList = new ArrayList<>();
        //新增日活
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", esService.getDauTotal(dt));
        rsList.add(dauMap);
        //新增设备
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", esService.getNewMidTotal(dt));
        rsList.add(newMidMap);
        return rsList;
    }

    //日活的分时查询
    @RequestMapping("/readtime-hour")
    public Object readtimeHour(@RequestParam(value = "id", defaultValue = "-1") String id,
                               @RequestParam("date") String dt) {
        if (id.equals("dau")) {
            //封装返回的数据
            Map<String, Map> hourMap = new HashMap<>();
            //获取今天日活分时
            Map dauHourTdMap = esService.getDauHour(dt);
            hourMap.put("today", dauHourTdMap);
            //获取昨天日期
            String yd = getYd(dt);
            //获取昨天日活分时
            Map dauHourYdMap = esService.getDauHour(yd);
            hourMap.put("yesterday", dauHourYdMap);
            return hourMap;
        }
        return null;
    }
    private String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }
}
