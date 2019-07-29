package poc

import java.nio.file.Files
import java.util.Properties

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProviderChain, EnvironmentVariableCredentialsProvider}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import util.GetAllProperties

object ReadWriteRDDToS3 {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // get input file location var
    var inputFile = GetAllProperties.readPropertyFile get "INPUT_FILE" getOrElse("#")

    val awsCredentials = new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(), new ProfileCredentialsProvider())

    //init spark conf and spark context
    val conf = new SparkConf(true).setMaster("local[1]").setAppName("ReadAndWriteRDDToS3")

    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("fs.s3.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", awsCredentials.getCredentials.getAWSAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", awsCredentials.getCredentials.getAWSSecretKey)

    //ways to create RDD
    val fileRDD = sc.textFile(inputFile)

    println(fileRDD.take(10))

    for( i <- 1 to 100){

      fileRDD.saveAsTextFile(s"<S3_BUCKET_PATH>/$i/")
    }

  }

}
