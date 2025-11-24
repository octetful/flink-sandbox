package com.ktech.repertoire.streaming;

public class Onboarding {
  private String correlationId;
  private int totalParts;
  private String scheduleId;

  public String getCorrelationId() {
    return correlationId;
  }

  public void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  public String getScheduleId() {
    return scheduleId;
  }

  public void setScheduleId(String scheduleId) {
    this.scheduleId = scheduleId;
  }

  public int getTotalParts() {
    return totalParts;
  }

  public void setTotalParts(int totalParts) {
    this.totalParts = totalParts;
  }
}
