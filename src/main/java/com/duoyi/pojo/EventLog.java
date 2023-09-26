package com.duoyi.pojo;

import java.util.Map;

public class EventLog {
    private long guid;
    private Integer sessionId;
    private String eventId;
    private long timeStamp;
    private Map<String,String> eventInfo;


    public long getGuid() {
        return guid;
    }

    public void setGuid(long guid) {
        this.guid = guid;
    }

    public Integer getSessionId() {
        return sessionId;
    }

    public void setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Map<String, String> getEventInfo() {
        return eventInfo;
    }

    public void setEventInfo(Map<String, String> eventInfo) {
        this.eventInfo = eventInfo;
    }
}
