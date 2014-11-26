package com.zjhcsoft.lfp

import java.util.concurrent.{ExecutorService, Executors, CountDownLatch}

import akka.actor.{ActorLogging, Actor}
import akka.event.Logging


class LineProcessor(processFun: (Array[String] => Unit),counter:CountDownLatch) extends Actor with ActorLogging{

  def receive = {
    case lines: Array[String] =>
      if(LineProcessor.async){
        LineProcessor.executor.execute(new Runnable {
          override def run(): Unit = {
            execute(lines)
          }
        })
      }else{
        execute(lines)
      }
  }

  def execute(lines: Array[String]) {
    processFun(lines)
    counter.countDown()
    log.debug("Counter remainder :" + counter.getCount)
  }

}

object LineProcessor{

  //是否异步处理，默认为同步（这意味着actor会被自定义处理函数阻塞）
  //异步可能导致内存溢出，即本程序不断地产生分段的内存映射，而自定义处理函数可能无法及时消费这些数据
  var async = false

  private[LineProcessor] var executor: ExecutorService =Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2)

  def init(threadNumber: Int): Unit ={
    executor=  Executors.newFixedThreadPool(threadNumber)
  }

}

