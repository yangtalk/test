package com.duoyi.pojo;

public class Student {
    private Integer id;
    private String  name;
    private String  gender;
    private Float score;

    public Integer getId() {
        return id;
    }

    public Student() {
    }

    public Student(Integer id, String name, String gender, Float score) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.score = score;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }
}
