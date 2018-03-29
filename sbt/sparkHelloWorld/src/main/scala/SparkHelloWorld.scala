import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager, Logger}

object sparkHW {
    def main(args: Array[String]) {

        Logger.getLogger("org").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.info("Start")
        println("cat in the hat")
        if (args.length < 1) {
            log.error("Missing argument")
            return
        }
        val outputFile = args(0)
        val conf = new SparkConf().setAppName("Spark Hello World")
        val sc = new SparkContext(conf)
        val rdd = sc.parallelize(1 to 10)
        rdd.saveAsTextFile(outputFile)
        log.info("End")
        sc.stop()
    }
}