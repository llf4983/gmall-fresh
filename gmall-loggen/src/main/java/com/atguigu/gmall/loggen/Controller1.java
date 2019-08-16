package com.atguigu.gmall.loggen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller@ResponseBody
@RestController
@Slf4j
public class Controller1 {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping
    public String doLog(@RequestParam String logString){
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        String logJson = jsonObject.toJSONString();
        log.info(logJson);

        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.TOPIC_START,logJson);
        }else {
            kafkaTemplate.send(GmallConstants.TOPIC_EVENT,logJson);

        }
        return "seccess";
    }
}
