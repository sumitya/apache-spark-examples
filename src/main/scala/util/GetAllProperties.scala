package util

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConverters._

object GetAllProperties {

    private val prop = new Properties()
    private var properties  = Map[String, String]()

      def readPropertyFile(): Map[String, String] = {

        val userName = System.getProperty("user.name")

        prop.load(new FileInputStream(s"C://Users//"+userName+"//intelliJProjects//apache-spark-examples1//apache-spark-examples//src//main//resources//app.properties"))

        prop.entrySet().asScala.foreach {
          (entry) => {
            properties += ((entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String]))
          }
        }
        properties
      }
}
