package com.atguigu.gmall0105.publisher.bean;

import java.util.List;
import java.util.Map;

public class SaleInfo {
     Long total;
     List<OptionGroup>  stat;  //多个饼图
     List<Map> detail;


    public SaleInfo(Long total, List<OptionGroup> stat, List<Map> detail) {
        this.total = total;
        this.stat = stat;
        this.detail = detail;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public List<OptionGroup> getStat() {
        return stat;
    }

    public void setStat(List<OptionGroup> stat) {
        this.stat = stat;
    }

    public List<Map> getDetail() {
        return detail;
    }

    public void setDetail(List<Map> detail) {
        this.detail = detail;
    }
}
