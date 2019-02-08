//package com.lamresearch
//import java.io.FileInputStream
//import java.net.URI
//import java.util.Properties
//import java.text.SimpleDateFormat
//import java.sql.Timestamp
//
//import org.apache.spark.sql.functions.udf
//
//import scala.util.{Failure, Success, Try}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.{LogManager, Logger}
//import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, TimestampType}
//import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, TimestampType}
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//object DataIngestionLam {
//  def main(args: Array[String]): Unit = {
//
//
//
//    //    val conf = new SparkConf().setMaster("local[2]").setAppName("Kafka_Spark_Hbase").set("spark.ui.port", "4050")
//    //    val ssc = new StreamingContext(conf, Seconds(60))
//    val spark=SparkSession.builder().appName("ccc").master("local").getOrCreate()
//
//    //        val brokers="atlhashed01.hashmap.net:6667"
//    //        val offsetResetType="latest"
//    //        val groupId="use_a_separate_group_id_for_each_stream"
//    //        val zookeeper="atlhashdn01.hashmap.net:2181,atlhashdn02.hashmap.net:2181,atlhashdn03.hashmap.net:2181"
//    //        val kafkaParams = Map[String, String](
//    //          "metadata.broker.list" -> brokers,
//    //          "auto.offset.reset"-> offsetResetType,
//    //          "group.id"->groupId,
//    //          "zookeeper.connect"->zookeeper)
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "atlhashed01.hashmap.net:6667",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "use_a_separate_group_id_for_each_stream",
//      "auto.offset.reset" -> "earliest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
//    val topics = Array("files") //topics list
//    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams))
//    //    print(kafkaStream)
//    print("hello")
//    val stream: DStream[String] =kafkaStream.map(x=>x.value().toString)
//    stream.print()
//    stream.map(x=> val df = spark.read.option("multiline", "true")
//      .option("header","true")
//      .option("inferSchema","true")
//      .json(x)
//
//    val cols=df.columns.toList
//
//    //df.printSchema()
//    if(cols.contains("meta")&&cols.contains("recipe_steps"))
//      OES(df,spark,log)
//    else
//      WDL(df,spark,log)
//    val filelistkafka: Seq[String]
//
//
//
//    val log: Logger = LogManager.getRootLogger()
//    val prop: Properties = new Properties()
//    val spark = SparkSession.builder()
//      .appName("DataIngestionLam")
//      .master("local")
//      .getOrCreate()
//
//
//    spark.sparkContext.setLogLevel("WARN")
//    log.info("JSON File Load Start !!")
//    println("args=="+args.length)
//    val configProps = args(0)
//    val srcFile = args(1)
//    val target=args(2)
//    prop.load(new FileInputStream(configProps))
//    val zookeeperUrl = prop.getProperty("zkUrl")
//    val metaTableName = prop.getProperty("meta.tableName")
//    val tsTableName = prop.getProperty("timeSeries.tableName")
//
//
//    println(zookeeperUrl)
//    println(metaTableName)
//    println(srcFile)
//
//
//    //    val srcFile="C:\\Users\\nk224\\Downloads\\OES Json Input Files-20190123T064511Z-001\\OES Json Input Files"
//    //    val target="C:\\Users\\nk224\\Desktop\\puf"
//    val fileList: Seq[String] =getFileList(srcFile)
//
//    if (filelistkafka.isEmpty) {
//      println("Folder Empty")
//    } else {
//      filelistkafka.foreach(println)
//      for (i <- filelistkafka.indices) {
//
//        val fileObj = filelistkafka(i)
//        val pathOfFile = fileObj
//
//        if (fileList contains pathOfFile)
//        {
//          val df = spark.read.option("multiline", "true")
//            .option("header","true")
//            .option("inferSchema","true")
//            .json(pathOfFile)
//
//          val cols=df.columns.toList
//
//          //df.printSchema()
//          if(cols.contains("meta")&&cols.contains("recipe_steps"))
//            OES(df,spark,log)
//          else
//            WDL(df,spark,log)
//        }
//        else
//        {
//          print("File not found in hdfs")
//        }
//        moveFileToArchive(pathOfFile, target)
//      }
//    }
//    import spark.implicits._
//    //    val df = spark.read.option("multiline", "true")
//    //                        //.option("header","true")
//    //                        .option("inferSchema","true")
//    //      .json("C:\\Users\\nk224\\Downloads\\OES Json Input Files-20190123T064511Z-001\\OES Json Input Files")
//    //    df.printSchema()
//    //$"recipe_steps.OES_spectra.Spectra".alias("Time"),$"recipe_steps.OES_spectra.Spectra".alias("Values")
//    //    val metadataDF=df.select($"meta.recipe_ID".alias("recipe_ID"),$"meta.tool_ID".alias("tool_id"),$"meta.chamber_id".alias("chamber_id"),$"meta.slot_ID".alias("Slot_id"),$"meta.wafer_ID".alias("wafer_ID"))
//    //    val wavelengthDF=df.select($"recipe_steps.OES_spectra.Wavelengths".alias("Wavelengths"))
//    //    val wavelengthDFfinal=wavelengthDF.select(explode($"Wavelengths") as "Wavelengths")
//    //
//    //
//    //    metadataDF.show()
//    //    wavelengthDFfinal.show(false)
//
//    //val t1=df.select(explode($"recipe_steps.OES_spectra.Spectra") as "time")
//
//
//    //    case class on1(Step:Int,
//    //    Channel :String,
//    //    Time : String,
//    //    Values:Array[Int])
//    //    val t1: DataFrame =df.select(explode($"recipe_steps.OES_spectra.Spectra") as "time")
//    //    val t2=t1.select(col("time") as("data")).withColumn("date",explode(col("data")))
//    //    val valuesAndTime=t2.select(col("data").getItem(0).as("data"),col("date"))
//    //    valuesAndTime.select($"data.Step",$"date.Time",$"date.Values").show(100,false)
//    ssc.start()
//    ssc.awaitTermination()
//  }
//  def getFileList(path: String): List[String] = {
//    val uri = new URI("hdfs:///")
//    val fs = FileSystem.get(uri, new Configuration())
//    val filePath = new Path(path)
//    val files = fs.listFiles(filePath, false)
//    var fileList = List[String]()
//
//    while (files.hasNext) {
//      fileList ::= files.next().getPath.toString
//    }
//    fileList
//  }
//
//  def moveFileToArchive(inPath: String, toPath: String): Unit = {
//    val uri = new URI("hdfs:///")
//    val file = inPath.split("/")
//    val filename = file(file.length - 1)
//    val fs = FileSystem.get(uri, new Configuration())
//    val sourcePath = new Path(inPath)
//    val destPath = new Path(toPath + "/" + filename)
//    println(sourcePath)
//    println(destPath)
//    fs.rename(sourcePath, destPath)
//    // println("moved")
//  }
//  def WDL(inputDf:DataFrame,spark: SparkSession,log:Logger): Unit =
//  {
//    import spark.implicits._
//    val metaPKDF = inputDf.select(col("process_executions.header.process_job_ID")
//      .alias("PJID"), col("process_executions.header.wafer_ID")
//      .alias("WID"), col("process_executions.header.lot_ID")
//      .alias("LID"), col("process_executions.header.slot_ID")
//      .alias("SLTID"), col("process_executions.header.recipe_ID")
//      .alias("RCPID"), col("process_executions.header.tool_ID")
//      .alias("TID"), col("process_executions.header.chamber_ID")
//      .alias("CBRN"), col("process_executions.header.start_time")
//      .alias("RSTS"))
//
//    val metadataWithFNDF = metaPKDF.withColumn("MMID", hash(metaPKDF.columns.map(col): _*))
//      .withColumn("WDFLN", lit(inputDf.inputFiles(0)))
//
//    val metadataDF: DataFrame = metadataWithFNDF.select(col("PJID").getItem(0).alias("PJID"),
//      col("WID").getItem(0).alias("WID"),
//      col("LID").getItem(0).alias("LID"),
//      col("SLTID").getItem(0).cast(IntegerType).alias("SLTID"),
//      col("MMID"),
//      col("RCPID").getItem(0).alias("RCPID"),
//      col("TID").getItem(0).alias("TID"),
//      col("CBRN").getItem(0).alias("CBRN"),
//      col("WDFLN"),
//      col("RSTS").getItem(0).cast("timestamp").alias("RSTS"))
//
//    val aryMMID = metadataDF.limit(1).select("MMID").as[Integer].collect()
//    val intMMID = aryMMID(0)
//
//    //Save to Phoenix
//    //    val zookeeperUrl = "jdbc:phoenix:atlhashdn01.hashmap.net,atlhashdn02.hashmap.net,atlhashdn03.hashmap.net:2181"
//    //    val metaTableName = "MATERIAL_META1"
//    //    val tsTableName = "TS_T1"
//
//
//    //    try{
//    //      metadataDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", metaTableName).option("zkUrl", zookeeperUrl).save()
//    //    } catch {
//    //      case ph: Throwable => log.error("Unable to write data into Phoenix table " + metaTableName + ". Exception: "+ ph.getMessage)
//    //    }
//
//
//    log.info("Meta Data is inserted into Phoenix!")
//
//    val arrayTSDF0 = inputDf.select(col("process_executions.data").getItem(0).alias("data1"))
//    val arrayTSDF1 = arrayTSDF0.select(explode(col("data1")))
//
//    val arrayTSDF2 = arrayTSDF1.withColumn("TSData", struct(col("col.step").alias("tsstep"), col("col.parameters").alias("tsParameters"))).select("TSData")
//    val arrayTSDF3 = arrayTSDF2.select(col("TSData.tsstep").alias("tsstep"), col("TSData.tsParameters").alias("tsParameters"))
//    val arrayTSDF4 = arrayTSDF3.select(col("tsstep"), explode(col("tsParameters")).alias("tsParameters"))
//    val arrayTSDF5 = arrayTSDF4.select(col("tsstep"), col("tsParameters.parameter_name").alias("tsParaName"), col("tsParameters.samples").alias("tsSamples"))
//    val arrayTSDF6 = arrayTSDF5.select(col("tsstep"), col("tsParaName"), explode(col("tsSamples")).alias("tsSamples"))
//    val arrayTSDF7 = arrayTSDF6.select(col("tsstep").alias("STPID"), col("tsParaName").alias("CID"), col("tsSamples.time").cast(TimestampType).alias("TS"), col("tsSamples.value").cast(DoubleType).alias("VL"))
//    val arrayTSDF8 = arrayTSDF7.withColumn("MMID", lit(intMMID))
//
//    val finalTSDF = arrayTSDF8.select(col("MMID"), col("CID"), col("TS"), col("VL"), col("STPID"))
//    finalTSDF.show(20,false)
//    finalTSDF.printSchema()
//    //    try{
//    //      finalTSDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", tsTableName).option("zkUrl", zookeeperUrl).save()
//    //    } catch {
//    //      case ph: Throwable => log.error("Unable to write data into Phoenix table " + tsTableName + ". Exception: "+ ph.getMessage)
//    //    }
//
//
//    log.info("Time Series Data is inserted into Phoenix!")
//
//    log.info("WDL Ingestion is Completed !!")
//  }
//
//  def OES(inputDf:DataFrame,spark: SparkSession,log:Logger): Unit =
//  {
//    import spark.implicits._
//
//    val getTimestamp: (String => Option[Timestamp]) = s => s match {
//      case "" => None
//      case _ => {
//        val format = new SimpleDateFormat("dd-MM-yyyy'T'HH:mm:ss.SSS'Z'")
//        Try(new Timestamp(format.parse(s).getTime)) match {
//          case Success(t) => Some(t)
//          case Failure(_) => None
//        }
//      }
//    }
//    val getTimestampUDF = udf(getTimestamp)
//    //val inputDf = spark.read.option("multiline", "true").option("header","true").option("inferSchema","true").json("/user/vimal/spectrafiles/nw1.json")
//    val metaPKDF = inputDf.select(col("meta.lot_ID").alias("LID"), col("meta.slot_ID").alias("SLTID").cast(IntegerType), col("meta.wafer_ID").alias("WID"), col("meta.recipe_ID").alias("RCPID"), col("meta.tool_ID").alias("TID"), col("meta.chamber_id").alias("CBRN"), col("meta.process_job_ID").alias("PJID"), col("meta.start_time").alias("RSTS"))
//    val metadataWithFNDF = metaPKDF.withColumn("MMID", hash(metaPKDF.columns.map(col): _*)).withColumn("WDFLN", lit(inputDf.inputFiles(0))).withColumn("SID", lit(1))
//
//    val metadataDF: DataFrame = metadataWithFNDF.select(col("PJID"), col("WID"), col("SID"), col("LID"), col("SLTID").cast(IntegerType).alias("SLTID"), col("MMID"), col("RCPID"), col("TID"), col("CBRN"), col("WDFLN"), col("RSTS").cast(TimestampType).alias("RSTS"))
//    //metadataDF.show(20)
//
//    val aryMMID = metadataDF.limit(1).select("MMID").as[Integer].collect()
//    val intMMID = aryMMID(0)
//
//    //    try{
//    //      metadataDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", metaTableName).option("zkUrl", zookeeperUrl).save()
//    //    } catch {
//    //      case ph: Throwable => log.error("Unable to write data into Phoenix table " + metaTableName + ". Exception: "+ ph.getMessage)
//    //    }
//
//    log.info("Meta Data is inserted into Phoenix!")
//
//    val wavelengthDF = inputDf.select(col("recipe_steps.OES_spectra.Wavelengths").alias("Wavelengths"), col("recipe_steps.OES_spectra.Spectra").alias("arrayVal"), col("recipe_steps.Step").alias("step"))
//    val arrayDataDF0 = wavelengthDF.select(col("Wavelengths"), col("arrayVal")).withColumn("Wavelengths", col("Wavelengths")(0))
//    val arrayDataDF1 = arrayDataDF0.select(col("Wavelengths"), explode(col("arrayVal")).as("arrayVal"))
//    val arrayDataDF2 = arrayDataDF1.select(col("Wavelengths"), explode(col("arrayVal")).as("arrayVal"))
//
//    val arrayDataDF3 = arrayDataDF2.select(col("Wavelengths"), col("arrayVal.Step").alias("Step"), col("arrayVal.Time").alias("Time"), col("arrayVal.Values").alias("Values")).withColumn("Time",getTimestampUDF(col("Time")))//.withColumn("Time", unix_timestamp($"Time", "dd-MM-yyyy'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
//    arrayDataDF3.select("Time").show(20,false)
//    val arrayDataDF4 = arrayDataDF3.withColumn("valuesExploded", explode(arrayDataDF3("Values"))).select(col("Step").as("Step"),col("Time").as("Time"),col("valuesExploded").as("Values"))
//    val arrayDataDF5 = arrayDataDF3.withColumn("wavelengthExploded", explode(arrayDataDF3("Wavelengths"))).select(col("Step").as("Step1"),col("Time").as("Time1"),col("wavelengthExploded").as("Wavelengths"))
//
//    val arrayDataDF44 = arrayDataDF4.withColumn("index", monotonically_increasing_id)
//    val arrayDataDF55 = arrayDataDF5.withColumn("index", monotonically_increasing_id)
//
//    val arrayDataDF6 = arrayDataDF44.join(arrayDataDF55, arrayDataDF44("index") === arrayDataDF55("index"), "outer").drop("index").drop("Step1").drop("Time1")
//
//    val arrayDataDF7 = arrayDataDF6.select(col("Step").cast(StringType).alias("STPID"), col("Wavelengths").cast(StringType).alias("CID"), col("Time").alias("TS"), col("Values").cast(DoubleType).alias("VL"))
//    val arrayDataDF8 = arrayDataDF7.withColumn("MMID", lit(intMMID))
//
//    val finalTSDF = arrayDataDF8.select(col("MMID"), col("CID"), col("TS"), col("VL"), col("STPID"))
//    finalTSDF.show(20,false)
//
//    //    try{
//    //      finalTSDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", tsTableName).option("zkUrl", zookeeperUrl).save()
//    //    } catch {
//    //      case ph: Throwable => log.error("Unable to write data into Phoenix table " + tsTableName + ". Exception: "+ ph.getMessage)
//    //    }
//
//    //finalTSDF.show(20,false)
//    log.info("Time Series Data is inserted into Phoenix!")
//
//    log.info("OES Ingestion is Completed !!")
//  }
//
//}
