package com.duoyi.pojo;


import scala.Int;

public class CountBean {
    private String word;
    private Integer count;

    public String getWord() {
        return word;
    }

    public CountBean(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public CountBean() {
    }

    @Override
    public String toString() {
        return "CountBean{" +
                "word='" + word + '\'' +
                ", count='" + count + '\'' +
                '}';
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
