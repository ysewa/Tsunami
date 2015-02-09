import java.io.StringReader
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVParser

/***************************************************************/
// SET THE WARNING LEVEL
import org.apache.log4j.{Level,Logger}
import org.apache.log4j.{Level, Logger}
val level=Level.WARN
Logger.getLogger("org").setLevel(level)
Logger.getLogger("akka").setLevel(level)
/****************************************************************/

def ArrondisDate(t:String,y:SimpleDateFormat):String ={
val tmp = y.parse(t);
val minutes= tmp.getMinutes();
tmp.setMinutes(minutes+10-minutes%10);
tmp.setSeconds(0);
return y.format(tmp);
}


//val JapanData = sc.textFile("data_1MB.csv",5).cache()


val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3),columns(2).toFloat,columns(3).toFloat),(columns(4).toInt))
         })
       }).groupByKey().map(x=>(x._1._1,x._1._2,x._1._3,x._1._4,x._2)).saveToCassandra("test","test_spark_set")
  
  //Commandes tests     
result.groupByKey(20).map(x=>(x._1._1,x._1._2,x._1._3,x._1._4,x._2)).take(2)
val resultWithKey = result.map(x => (x._1+" "+x._2,x))
val sortedresultWithKey = resultWithKey.sortByKey()



result.saveToCassandra("kspace","tableName")

val parser = new CSVParser(';')
val result = JapanData.map(line => {
           val columns = parser.parseLine(line)
           (columns(0).substring(0,15)+"0:00",columns(1).substring(0,3),columns(4).toInt,columns(2).toFloat,columns(3).toFloat)
         })
       })
    
/*
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader


val result2 = JapanData.map{ line => val reader = new CSVReader(new StringReader(line),';');reader.readNext();}

val transform = result2.map{x=> (ArrondisDate(x(0),simpleDateFormat),x(1).substring(0,3),x(4).toInt,x(2).toFloat,x(3).toFloat)}

*/
