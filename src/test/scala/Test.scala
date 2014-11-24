import com.zjhcsoft.lfp.LFP

object Test extends App {

  val s = System.currentTimeMillis()
  LFP("C:\\DATA\\Enjoy_Projects\\LargeFileProcessor\\src\\test\\resources\\test.txt", {
    lines =>
      lines.foreach(println(_))
  })
  println(System.currentTimeMillis() - s)
}

