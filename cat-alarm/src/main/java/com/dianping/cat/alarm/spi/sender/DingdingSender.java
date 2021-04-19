package com.dianping.cat.alarm.spi.sender;

import com.dianping.cat.alarm.spi.AlertChannel;

public class DingdingSender extends AbstractSender {

  public static final String ID = AlertChannel.DINGDING.getName();

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean send(SendMessageEntity message) {
    return false;
  }
}
