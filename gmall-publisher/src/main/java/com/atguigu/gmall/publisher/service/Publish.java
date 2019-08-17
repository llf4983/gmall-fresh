package com.atguigu.gmall.publisher.service;

import java.util.Map;

public interface Publish {
    public Long getTotal(String date);
    public Map<String,Long> getHour(String date);

    public Double orderTotal(String date);
    public Map<String,Double> orderHour(String date);
}
