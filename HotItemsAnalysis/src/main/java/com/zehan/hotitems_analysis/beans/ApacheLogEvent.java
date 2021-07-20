package com.zehan.hotitems_analysis.beans;

public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long timestamp;
    private String methode;
    private String url;

    public ApacheLogEvent(){}

    public ApacheLogEvent(String ip, String userId, Long timestamp, String methode, String url) {
        this.ip = ip;
        this.userId = userId;
        this.timestamp = timestamp;
        this.methode = methode;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMethode() {
        return methode;
    }

    public void setMethode(String methode) {
        this.methode = methode;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                ", methode='" + methode + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
