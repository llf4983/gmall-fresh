package com.atguigu.gmall.publisher.service;

import java.util.Map;

public interface Publish {
    public Long getTotal(String date);
    public Map<String,Long> getHour(String date);
}
