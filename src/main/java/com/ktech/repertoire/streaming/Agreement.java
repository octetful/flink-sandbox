package com.ktech.repertoire.streaming;

import java.util.List;

public class Agreement {
  private String correlationId;
  private List<String> scheduleIds;

  public String getCorrelationId() {
    return correlationId;
  }

  public void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  public List<String> getScheduleIds() {
    return scheduleIds;
  }

  public void setScheduleIds(List<String> scheduleIds) {
    this.scheduleIds = scheduleIds;
  }
}
