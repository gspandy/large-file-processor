package com.zjhcsoft.lfp

import java.util.concurrent.CountDownLatch

import akka.actor.Actor
import akka.event.Logging


class LineProcessor(processFun: (Array[String] => Unit),counter:CountDownLatch) extends Actor {

  private val logger = Logging(context.system, this)

  def receive = {
    case lines: Array[String] =>
      processFun(lines)
      counter.countDown()
      logger.debug("Counter remainder :" + counter.getCount)
  }

}

