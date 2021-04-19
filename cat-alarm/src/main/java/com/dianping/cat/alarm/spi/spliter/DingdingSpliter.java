package com.dianping.cat.alarm.spi.spliter;

import com.dianping.cat.alarm.spi.AlertChannel;

public class DingdingSpliter implements Spliter {

  public static final String ID = AlertChannel.DINGDING.getName();

  @Override
  public String getID() {
    return ID;
  }

  @Override
  public String process(String content) {
    return content;
  }
}
