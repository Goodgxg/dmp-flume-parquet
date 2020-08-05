package com.dmp.pansoft.flume.event;

import java.util.HashMap;
import java.util.Map;

public class WindowsLogEvent {
    public String EventTime;
    public String Hostname;
    public String EventType;
    public String Severity;
    public String SourceModuleName;
    public String UserID;
    public String ProcessID;
    public String Domain;
    public String EventReceivedTime;
    public String Path;
    public String Message;
    public Map<String, Object> dynamic = new HashMap<>();
}