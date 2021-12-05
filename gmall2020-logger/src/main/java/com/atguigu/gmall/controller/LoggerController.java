package com.atguigu.gmall.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

//标识为 controller 组件，交给 Sprint 容器管理，并接收处理请求 如果返回 String，会当作网页进行跳转
//@Controller
// @RestController = @Controller + @ResponseBody 会将返回结果转换为 json 进行响应
@RestController
@Slf4j
public class LoggerController {

    //注入Spring提供的kafka编程模板
    @Autowired

    KafkaTemplate<String, String> kafkaTemplate;

    //通过requestMapping匹配请求并交给方法处理
    //在模拟数据生成的代码中,我们将数据封装为json,通过post传递给该controller处理
    //所以我们通过@RequestBody接收
    @RequestMapping("/applog")
    public String appLog(@RequestBody String jsonLog) {
        log.info(jsonLog);//借助 Logbak 将采集的日志落盘
        //将不同类型日志发送到kafka主题中
        JSONObject jsonObject = JSON.parseObject(jsonLog);
        if (jsonObject.getJSONObject("start") != null) {
            //启动日志
            kafkaTemplate.send("gmall_start_0523", jsonLog);
        } else {
            //事件日志
            kafkaTemplate.send("gmall_event_0523", jsonLog);
        }
        return "success";
    }
}
