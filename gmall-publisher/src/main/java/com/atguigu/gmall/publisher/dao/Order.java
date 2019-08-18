package com.atguigu.gmall.publisher.dao;

import com.atguigu.gmall.publisher.bean.HourBean;

import java.util.List;

public interface Order {
    public Double getTol(String date);

    public List<HourBean> getHou(String date);

    
}
