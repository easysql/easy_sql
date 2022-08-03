package your.company

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

object udfs {
    def initUdfs(spark: SparkSession) {
        val string_set = udf((s: Seq[String]) => s.filter(_ != null).toSet.toArray)
        spark.udf.register("string_set", string_set)
    }
}
