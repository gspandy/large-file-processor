package com.zjhcsoft.lfp

import java.util.concurrent.{ExecutorService, Executors, CountDownLatch}

import akka.actor.Actor
import akka.event.Logging


class LineProcessor(processFun: (Array[String] => Unit),counter:CountDownLatch) extends Actor {

  private val logger = Logging(context.system, this)

  def receive = {
    case lines: Array[String] =>
      LineProcessor.executor.execute(new Runnable {
        override def run(): Unit = {
          processFun(lines)
          counter.countDown()
          logger.debug("Counter remainder :" + counter.getCount)
        }
      })
  }

}

object LineProcessor{

  private[LineProcessor] var executor: ExecutorService =Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2)

  def init(threadNumber: Int): Unit ={
    executor=  Executors.newFixedThreadPool(threadNumber)
  }

}

