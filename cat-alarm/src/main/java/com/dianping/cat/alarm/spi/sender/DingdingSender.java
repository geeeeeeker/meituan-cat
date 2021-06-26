package com.dianping.cat.alarm.spi.sender;

import com.dianping.cat.alarm.spi.AlertChannel;

/**
 * 国内诸多公司使用钉钉，扩展
 */
public class DingdingSender extends AbstractSender {

  public static final String ID = AlertChannel.DINGDING.getName();

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean send(SendMessageEntity message) {

    //对接钉钉

    return false;
  }
}
