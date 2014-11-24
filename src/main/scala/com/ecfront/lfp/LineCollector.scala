package com.zjhcsoft.lfp

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import akka.actor.{Actor, ActorRef}
import akka.event.Logging

/**
 * 行集合收集类
 * @param lineProcess 行数据处理
 */
class LineCollector(lineProcess: ActorRef) extends Actor {

  private val logger = Logging(context.system, this)

  def receive = {
    case (fileName: String, chunkStart: Int, chunkSize: Int) =>
      val file = new File(fileName)
      val channel = new RandomAccessFile(file, "r").getChannel()
      val mappedBuff = channel.map(FileChannel.MapMode.READ_ONLY, 0, file.length())

      //确定end position
      var endP = chunkSize
      if (endP >= file.length()) {
        endP = file.length().intValue - 1
      }

      val start = mappedBuff.get(chunkStart)
      val startPosition = rowOffsetProcess(chunkStart, mappedBuff, start, endP)
      val end = mappedBuff.get(endP)
      val endPosition = if ((endP != file.length() - 1)) rowOffsetProcess(endP, mappedBuff, end, endP) else endP
      val stringBuilder = new StringBuilder()
      for (i <- startPosition to endPosition) {
        stringBuilder.append(mappedBuff.get(i).asInstanceOf[Char])
      }
      lineProcess ! stringBuilder.toString.split('\n').filter(_.trim != "")
  }

  /**
   * 对齐行数据，charBuff会向前移动直到找到换行符，即确保从行首开始
   * @param startP 开始position
   * @param charBuff  MappedByteBuffer
   * @param start  开始字符
   * @param length 最大长度
   * @return 最近的行首position
   */
  private def rowOffsetProcess(startP: Int, charBuff: java.nio.MappedByteBuffer, start: Byte, length: Int): Int = {

    var s = start.asInstanceOf[Char]
    val position = startP
    var next = position

    if (position <= length) {
      while (s != '\n' && position > 0) {
        s = charBuff.get(next).asInstanceOf[Char]
        next = next - 1
      }
    }
    if (position != next) {
      next + 1
    } else {
      position
    }
  }

}

