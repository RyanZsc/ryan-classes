package com.ryan.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {
  def main(args: Array[String]): Unit = {

    //启动服务器接收数据
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据")

    //等待客户端的连接
    val client = server.accept()

    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val subTask = objIn.readObject().asInstanceOf[SubTask]
    val ints = subTask.compute()

    println("计算节点[9999]计算的结果：" + ints)

    in.close()
    client.close()
    server.close()
  }
}
