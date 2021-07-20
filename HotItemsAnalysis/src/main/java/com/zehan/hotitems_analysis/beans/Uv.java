package com.zehan.hotitems_analysis.beans;

public class Uv {
    private String methode;
    private Long windowEnd;
    private Long count;

    public Uv(String methode, Long windowEnd, Long count) {
        this.methode = methode;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public Uv() {
    }

    public String getMethode() {
        return methode;
    }

    public void setMethode(String methode) {
        this.methode = methode;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Uv{" +
                "methode='" + methode + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
