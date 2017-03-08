import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._
import org.apache.spark.sql._
import java.sql.Date
import java.util.Calendar
import java.sql.Timestamp
import java.text.SimpleDateFormat

//parameters
val ffactcal = Calendar.getInstance();
ffactcal.clear()
ffactcal.set(2017,1,1, 0, 0, 0);
var ffact = ffactcal.getTime()

val ifactcal = Calendar.getInstance();
ifactcal.clear()
ifactcal.set(2017, 0, 1, 0, 0, 0);
var ifact = ifactcal.getTime()

var zookeeperServer = "10.0.0.16:2181:/hbase-unsecure"

var deviceFilter = "10000000001AC"
var lptable = "HOURLYREADS_REV"
var startingHour = new java.util.Date(2017,01,31)
var endingHour = new java.util.Date(2017,02,01)

var validationSaldo = "SaldoATRCompleto"
var validationCCH = "CCHCompleta"

val sqlContext = new SQLContext(sc)

val formatter = new java.text.SimpleDateFormat("HH:mm:ss:SSS");
val formatter2 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

// 1-Validación de Saldo ATR -> Comprobar que existe lectura diaria para inicio y para fin del ciclo de facturación

val startValidation = new Timestamp(System.currentTimeMillis());

val timer = System.currentTimeMillis()

val start = sqlContext.phoenixTableAsDataFrame(
  table = "DAILYREADS",
  columns = Seq("METER_ID","FH","SOURCE","T_VAL_AE"),
  predicate = Some("METER_ID LIKE \'" + deviceFilter +"\' AND FH = TO_DATE(\'2017-01-01\') AND SOURCE = 0"),
    //predicate = Some("FH = TO_DATE(\'2017-01-01\') AND SOURCE = 0"),
  zkUrl = Some(zookeeperServer)
    )

val end = sqlContext.phoenixTableAsDataFrame(
  table = "DAILYREADS",
  columns = Seq("METER_ID","FH","SOURCE","T_VAL_AE"),
  predicate = Some("METER_ID LIKE \'" + deviceFilter +"\' AND FH = TO_DATE(\'2017-02-01\') AND SOURCE = 0"),
    //predicate = Some("FH = TO_DATE(\'2017-02-01\') AND SOURCE = 0"),
  zkUrl = Some(zookeeperServer)
    )

val join = (start.join(end, start("METER_ID") === end("METER_ID"),"outer")
            .select(start("METER_ID")
                    ,start("FH").alias("FH1")
                    ,start("T_VAL_AE").alias("T_VAL_AE1")
                    ,end("FH").alias("FH2")
                    , end("T_VAL_AE").alias("T_VAL_AE2"))).cache()
      
val endValidation = new Timestamp(System.currentTimeMillis());

val dateFormatted = formatter.format(new Date(endValidation.getTime() - startValidation.getTime()));

// INSERCION DE RESULTADOS INVALIDOS, EN ESTE ESCENARIO METER 0 REGISTROS

val startValidation = new Timestamp(System.currentTimeMillis());

val missing = join.where((join("T_VAL_AE1").isNull) || (join("T_VAL_AE2").isNull))

var prepareResult = (missing.select("METER_ID")   
    .withColumn("RESULT", lit("0")) 
    .withColumn("FH",lit(formatter2.format(ffact)))
    .withColumn("VALIDATION_ID", lit(validationSaldo))
    .withColumn("EXECUTION_DATE", lit(formatter2.format(System.currentTimeMillis()))) 
    .select("METER_ID","RESULT","FH","VALIDATION_ID","EXECUTION_DATE"))

prepareResult.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "VALIDATION_RESULT","zkUrl" ->  zookeeperServer))


val endValidation = new Timestamp(System.currentTimeMillis());

val dateFormatted = formatter.format(new Date(endValidation.getTime() - startValidation.getTime()));

// INSERCION DE RESULTADOS VALIDOS, EN ESTE ESCENARIO METER 0 REGISTROS

val startValidation = new Timestamp(System.currentTimeMillis());

val valid = join.where(((join("T_VAL_AE1").isNotNull) && (join("T_VAL_AE2").isNotNull)))

val prepareResult = (valid.select("METER_ID")   
    .withColumn("RESULT", lit("1")) 
    .withColumn("FH",lit(formatter2.format(ffact)))
    .withColumn("VALIDATION_ID", lit(validationSaldo))
    .withColumn("EXECUTION_DATE", lit(formatter2.format(System.currentTimeMillis()))) 
    .select("METER_ID","RESULT","FH","VALIDATION_ID","EXECUTION_DATE"))

(prepareResult.save("org.apache.phoenix.spark"
                    , SaveMode.Overwrite
                    , Map("table" -> "VALIDATION_RESULT","zkUrl" -> zookeeperServer)
                   )
)

val endValidation = new Timestamp(System.currentTimeMillis());


val dateFormatted = formatter.format(new Date(endValidation.getTime() - startValidation.getTime()));


// 2 - Validación de CCH_BRUTA -> Comprobar que existe una curva completa para todo el ciclo de facturación

val startValidation = new Timestamp(System.currentTimeMillis());

val timer = System.currentTimeMillis()

val meterList = join.select("METER_ID").distinct().collect()


var inClause = meterList.mkString("(\'","\',\'","\')")

inClause = inClause.replaceAll("\\\\[", "").replaceAll("\\\\]","")

val df = sqlContext.phoenixTableAsDataFrame(
  table = lptable,
  columns = Seq("METER_ID","VAL_AE"),
    predicate = Some("METER_ID IN " + inClause + " AND FH >= TO_DATE(\'2017-01-01\') AND FH < TO_DATE(\'2017-02-01\') AND SOURCE = 0"),     
  zkUrl = Some(zookeeperServer)
    )


 
val df2 = df.groupBy("METER_ID").count().withColumnRenamed("count","c").cache()


val total_hours = (ffact.getTime() - ifact.getTime())/1000/60/60


val endValidation = new Timestamp(System.currentTimeMillis());
val dateFormatted = formatter.format(new Date(endValidation.getTime() - startValidation.getTime()));


// INSERCION DE RESULTADOS INVALIDOS

val startValidation = new Timestamp(System.currentTimeMillis());

val nok = df2.where((df2("c") !== total_hours))

var prepareResult = (nok.select("METER_ID")   
    .withColumn("FH",lit(formatter2.format(ffact)))
    .withColumn("RESULT", lit("0"))     
    .withColumn("VALIDATION_ID", lit(validationCCH))
    .withColumn("EXECUTION_DATE", lit(formatter2.format(System.currentTimeMillis()))) 
    .select("METER_ID","RESULT","FH","VALIDATION_ID","EXECUTION_DATE"))

prepareResult.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "VALIDATION_RESULT","zkUrl" -> zookeeperServer))




val endValidation = new Timestamp(System.currentTimeMillis());

val dateFormatted = formatter.format(new Date(endValidation.getTime() - startValidation.getTime()));


// INSERCION DE RESULTADOS VALIDOS

val startValidation = new Timestamp(System.currentTimeMillis());

val ok = df2.where((df2("c") === total_hours))

var prepareResult = (ok.select("METER_ID")   
    .withColumn("FH",lit(formatter2.format(ffact)))
    .withColumn("RESULT", lit("1"))     
    .withColumn("VALIDATION_ID", lit(validationCCH))
    .withColumn("EXECUTION_DATE", lit(formatter2.format(System.currentTimeMillis()))) 
    .select("METER_ID","RESULT","FH","VALIDATION_ID","EXECUTION_DATE"))

prepareResult.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "VALIDATION_RESULT","zkUrl" -> zookeeperServer))


val formatter = new java.text.SimpleDateFormat("HH:mm:ss:SSS");
val dateFormatted = formatter.format(new Date(System.currentTimeMillis() - timer));

println (dateFormatted)










