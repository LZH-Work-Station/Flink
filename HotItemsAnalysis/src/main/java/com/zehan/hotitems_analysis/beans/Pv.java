package com.zehan.hotitems_analysis.beans;

import java.sql.Timestamp;

public class Pv {
    private String methode;
    private Long count;
    private Long timestamp;

    public Pv(String methode, Long count, Long timestamp) {
        this.methode = methode;
        this.count = count;
        this.timestamp = timestamp;
    }

    public Pv() {
    }

    public String getMethode() {
        return methode;
    }

    public void setMethode(String methode) {
        this.methode = methode;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Pv{" +
                "methode='" + methode + '\'' +
                ", count=" + count +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
