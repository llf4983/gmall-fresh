package com.atguigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.publisher.service.Publish;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class Controller {
    @Autowired
    Publish publish;

@GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date){
//        JSON.toJSONString()
        List<Map> list=new ArrayList<>();
        Map map=new HashMap();
        Map map2=new HashMap();
        Map map3=new HashMap();
        Long total = publish.getTotal(date);
        map.put("id","dau");
        map.put("name","新增日活");
        map.put("value",total);

        map2.put("id","new_mid");
        map2.put("name","新增设备");
        map2.put("value",233);

        map3.put("id","order_amount");
        map3.put("name","交易额");
        map3.put("value",publish.orderTotal(date));
        list.add(map);
        list.add(map2);
        list.add(map3);

        return JSON.toJSONString(list);
    }


    @GetMapping("realtime-hour")
    public String getHour(@RequestParam("id") String id ,@RequestParam("date") String date){
        if("dau".equals(id)){
            Map<String, Long> today = publish.getHour(date);
            String yest = transformDate(date);
            Map<String, Long> yesterday = publish.getHour(yest);
            Map stringStringHashMap = new HashMap();

            stringStringHashMap.put("today",today);
            stringStringHashMap.put("yesterday",yesterday);
            return JSON.toJSONString(stringStringHashMap);

        }else if("order_amount".equals(id)){
            Map<String, Double> today = publish.orderHour(date);
            String yest = transformDate(date);
            Map<String, Double> yesterday = publish.orderHour(yest);
            Map hashMap = new HashMap();
            hashMap.put("yesterday",yesterday);
            hashMap.put("today",today);
            return JSON.toJSONString(hashMap);
        }else{

        }
        return null;

    }

    private String transformDate(String date){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date today=null;
        Date yesterday=null;
        String yest=null;
        try {
            today = simpleDateFormat.parse(date);
            yesterday = DateUtils.addDays(today, -1);
            yest = simpleDateFormat.format(yesterday);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yest;
    }
}
