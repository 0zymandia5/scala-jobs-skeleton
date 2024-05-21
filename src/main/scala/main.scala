package com.spark.main

import org.apache.spark.sql.DataFrame
import scala.io.StdIn.readLine
import libs.files
import jobs._

class Item(var name: String, var value: String) {}

object main {
  var topicID = "";
  var topicVal = "";

  def main(args: Array[String]): Unit = {

    val files = new files();
    val options: Array[Item] =
      Array(new Item("TopicType", ""), new Item("JobID", ""))

    try {
      files._init_files_utils;
      var menuDF: DataFrame = files.readCSV("./src/main/behaviors/menu.csv");

      if (args.length == 0) {
        // No parameters assigned
        menuSelection(options, menuDF);
      } else if (args.length == 1) {
        // Only Topic Type parameter
        options(0).value = args(0);
        menuSelection(
          options.slice(1, 2),
          menuDF.filter(menuDF("TopicType") === args(0))
        );
      } else if (args.length == 2) {
        // Topic Type and Job ID parameters
        options(0).value = args(0);
        options(1).value = args(1);
        displayMenu(
          menuDF.filter(
            menuDF("TopicType") === args(0) && menuDF("JobID") === args(1)
          )
        );

        var targetPckg = menuDF
          .filter(
            menuDF(options(0).name) === options(0).value &&
              menuDF(options(1).name) === options(1).value
          )
          .collect()
          .toArray

        runJob(targetPckg.head.get(4), targetPckg.head.get(5))
      }

    } catch {
      case e: Exception => println("Exception Occurred : " + e)
    } finally {
      files.stopSparkSession;
    } // finally closure
  } // def main closure

  def displayMenu(menuDF: DataFrame): Unit = {
    println(
      "************ \n************ \nMain Menu! \n************ \n************"
    );
    menuDF.show();
  }

  def menuSelection(options: Array[Item], menuDF: DataFrame): Unit = {

    displayMenu(menuDF)
    for (opt <- options) {

      if (opt.name == "JobID") {
        println(s"Enter a ${opt.name} :")
        opt.value = readLine();
        displayMenu(
          menuDF.filter(
            menuDF(options(0).name) === options(0).value &&
              menuDF(opt.name) === opt.value
          )
        );
        var targetPckg = menuDF
          .filter(
            menuDF(options(0).name) === options(0).value &&
              menuDF(opt.name) === opt.value
          )
          .collect()
          .toArray

        runJob(targetPckg.head.get(4), targetPckg.head.get(5))
      } else {
        println(s"Enter a ${opt.name} :")
        opt.value = readLine();
        displayMenu(menuDF.filter(menuDF(opt.name) === opt.value));
      }

    }

  }

  def runJob(class_name: Any, job_name: Any): Unit = {
    val testInst = Class.forName(class_name.toString).newInstance()
    val method = testInst.getClass.getMethod(job_name.toString)
    method.invoke(testInst)
  }

} // object main closure
