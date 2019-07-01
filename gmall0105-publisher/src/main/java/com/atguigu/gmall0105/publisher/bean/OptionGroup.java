package com.atguigu.gmall0105.publisher.bean;

import java.util.List;

//饼图
public class OptionGroup {
        String title;

        List<Option> options; //多个选择

    public OptionGroup(String title, List<Option> options) {
        this.title = title;
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }
}
