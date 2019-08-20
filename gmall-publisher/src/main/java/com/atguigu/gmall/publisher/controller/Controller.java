package com.atguigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.publisher.bean.Options;
import com.atguigu.gmall.publisher.bean.Stat;
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
//    @Autowired
//    Options options;

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

    @GetMapping("sale_detail")
    public String getSale(@RequestParam("date")String date,@RequestParam("startpage") int startPage,@RequestParam("size") int pageSize,@RequestParam("keyword") String keywords){
        Map esMap = publish.getES(date, keywords,startPage, pageSize);
        Map gender = (Map)esMap.get("gender");
        Long male  = (Long)gender.get("M");
        Long female = (Long) gender.get("F");
        long genderTotal = male + female;
        Double malePercentage = Math.round(male * 1000 / genderTotal) / 10.0;
        Double femalePercentage = Math.round(female * 1000 / genderTotal) / 10.0;
        ArrayList<Options> genderOptionsList = new ArrayList<>();
        Options maleObj = new Options("男", malePercentage);
        Options femaleObj = new Options("女", femalePercentage);
        genderOptionsList.add(maleObj);
        genderOptionsList.add(femaleObj);
        Stat genderPercentageResult = new Stat(genderOptionsList, "用户性别占比");

        Map age = (Map) esMap.get("age");
        Long age20down=0L;
        Long age20_30=0L;
        Long age30up=0L;
        for (Object o : age.keySet()) {
            Long count = (Long)age.get(o);
            Long key = Long.parseLong((String)o);
            if(key<20){
                age20down+=count;
            }else if(key<30){
                age20_30+=count;
            }else {
                age30up+=count;
            }

        }
        Long ageTotal=age20down+age20_30+age30up;
        double age20Perc = Math.round(age20down * 1000 / ageTotal) / 10.0;
        double age20_30Perc = Math.round(age20_30 * 1000 / ageTotal) / 10.0;
        double age30upPerc = Math.round(age30up * 1000 / ageTotal) / 10.0;
        ArrayList<Options> ageList = new ArrayList<>();
        ageList.add(new Options("20岁以下",age20Perc));
        ageList.add(new Options("20岁到30岁",age20_30Perc));
        ageList.add(new Options("30岁及30岁以上",age30upPerc));
        Stat agePercentageResult = new Stat(ageList, "用户年龄占比");
        ArrayList<Stat> statArrayList = new ArrayList<>();
        statArrayList.add(genderPercentageResult);
        statArrayList.add(agePercentageResult);

        HashMap resultMap = new HashMap();
        resultMap.put("total",esMap.get("total"));
        resultMap.put("stat",statArrayList);
        resultMap.put("detail",esMap.get("hits"));
        return JSON.toJSONString(resultMap);
    }

}
