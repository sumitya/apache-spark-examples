package custom

import org.apache.spark.Partitioner

class UsersPartitioner extends Partitioner {

  override def numPartitions: Int = 10

  override def getPartition(key: Any): Int = {

    val userId = key.asInstanceOf[String].toInt

    if (userId == 0 || userId == 1) {
      return 0
    }
    else {
      return new java.util.Random().nextInt(2) + 1
    }
  }
}
