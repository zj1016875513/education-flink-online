package com.atguigu.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._

object RegisterStreaming {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    val consumerProps = new Properties()
    consumerProps.setProperty("bootstrap.servers", "k8s101:9092,k8s102:9092,k8s103:9092")
    consumerProps.setProperty("group.id", "test-group")
    consumerProps.setProperty("enable.auto.commit", "false")

    val source = new FlinkKafkaConsumer010[String]("register_topic", new SimpleStringSchema(), consumerProps)
    source.setStartFromEarliest()
    val registerStream = env.addSource[String](source)
    registerStream.map(item => {
      val array = item.split("\t")
      val app_name = array(1) match {
        case "1" => "PC"
        case "2" => "APP"
        case _ => "Other"
      }
      (app_name, 1)
    }).keyBy(item=>item._1).sum(1).print()
    env.execute()
  }


}

