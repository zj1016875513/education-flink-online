package com.atguigu.education.etl

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object TestMySql {

  case class Student(
                      id: Int,
                      name: String,
                      school_id: Int,
                      age: Int
                    )

  case class School(
                     id: Int,
                     name: String
                   )

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val source1 = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://hadoop101:3306/test2")
      .setUsername("root")
      .setPassword("000000")
      .setQuery("select id,name,school_id,age from students")
      .setRowTypeInfo(new RowTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO
      ))
    val source2 = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://hadoop101:3306/test2")
      .setUsername("root")
      .setPassword("000000")
      .setQuery("select id,name from school")
      .setRowTypeInfo(new RowTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ))
    val ds1 = env.createInput(source1.finish()).map(item => (new Student(Int.unbox(item.getField(0)), String.valueOf(item.getField(1)), Int.unbox(item.getField(2)), Int.unbox(item.getField(3)))))
    val ds2 = env.createInput(source2.finish()).map(item => new School(Int.unbox(item.getField(0)), String.valueOf(item.getField(1))))
    val result = ds1.join(ds2).where("school_id")
      .equalTo("id")
      .apply(new JoinFunction[Student, School, String] {
        override def join(first: Student, second: School): String = {
          val userid = first.id
          val name = first.name
          val age = first.age
          val schoolname = second.name
          "userid:" + userid + ",name:" + name + ",age:" + age + ",school:" + schoolname
        }
      })
    result.print()
  }
}
