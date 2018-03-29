import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameNaFunctions
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.log4j.{Level, LogManager, Logger}
import java.io._

object MushroomModel {
    def main(args: Array[String]) {

        Logger.getLogger("org").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.info("Start")
        println("...test print statement...")
        //if (args.length < 1) {
        //    log.error("Missing argument")
        //    return
        //}
        //val outputFile = args(0)

        val spark = SparkSession.builder.appName("Mushroom Model").getOrCreate()

        val schema = new StructType(Array(
            new StructField("classification", StringType, true),
            new StructField("capshape", StringType, true),
            new StructField("capsurface", StringType, true),
            new StructField("capcolor", StringType, true),
            new StructField("bruises", StringType, true),
            new StructField("odor", StringType, true),
            new StructField("gillattachment", StringType, true),
            new StructField("gillspacing", StringType, true),
            new StructField("gillsize", StringType, true),
            new StructField("gillcolor", StringType, true),
            new StructField("stalkshape", StringType, true),
            new StructField("stalkroot", StringType, true),
            new StructField("stalksurfaceabovering", StringType, true),
            new StructField("stalksurfacebelowring", StringType, true),
            new StructField("stalkcolorabovering", StringType, true),
            new StructField("stalkcolorbelowring", StringType, true),
            new StructField("veiltype", StringType, true),
            new StructField("veilcolor", StringType, true),
            new StructField("ringnumber", StringType, true),
            new StructField("ringtype", StringType, true),
            new StructField("sporeprintcolor", StringType, true),
            new StructField("population", StringType, true),
            new StructField("habitat", StringType, true)
        ))

        val data = spark.read.format("csv").schema(schema).option("header",false).load("s3://BUCKETNAME/input/mushrooms.csv")

        //removed veiltype because all mushrooms have same value for this feature
        val dropDF = data.na.drop("any")
        val params = "classification ~ capshape + capsurface + capcolor + bruises + odor + gillattachment + gillspacing + gillsize + gillcolor + stalkshape + stalkroot + stalksurfaceabovering + stalksurfacebelowring + stalkcolorabovering + stalkcolorbelowring + veilcolor + ringnumber + ringtype + sporeprintcolor + population + habitat"
        val myFormala = new RFormula().setFormula(params) 
        val fittedRF = myFormala.fit(dropDF)
        val preparedDF = fittedRF.transform(dropDF)

        val randSeed = 5043
        val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3), randSeed)

        val classifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(randSeed)
        val model = classifier.fit(train)
        val predictions = model.transform(test)

        model.write.save("s3://BUCKETNAME/output/mushroomModel")

        //val pw = new PrintWriter(new File("s3://BUCKETNAME/output/mushroomModel.txt" ))
        //pw.write(model.toDebugString)
        //pw.close

        val wrongPred = predictions.where(expr("label != prediction"))
        val countErrors = wrongPred.groupBy("label").agg(count("prediction").alias("Errors"))

        val rightPred = predictions.where(expr("label == prediction"))
        val countCorrect = rightPred.groupBy("label").agg(count("prediction").alias("Correct"))

        val falsevec = countErrors.select("Errors").collect()
        val falsepos = falsevec(0)
        val falseneg = 0.0
        val truevec = countCorrect.select("Correct").collect()
        val trueneg = truevec(0)
        val truepos = truevec(1)

        // import spark.implicits._
        //val cm = Seq(("tn", trueneg), ("fp", falsepos), ("fn", falseneg), ("tp", truepos)).toDF("name", "value")
        //val df = spark.createDataFrame(cm)
        //df.write.save("s3://cs696/output/mushroomConfusion")

        val cm = new PrintWriter(new File("s3://BUCKETNAME/output/mushroomConfusion"))
        cm.write(f"$trueneg $falsepos\n[$falseneg]  $truepos")
        cm.close

        log.info("End")
        spark.stop()
    }
}