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

val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

def ArrondisDate(t:String,y:SimpleDateFormat):String ={
val tmp = y.parse(t);
val minutes= tmp.getMinutes();
tmp.setMinutes(minutes+10-minutes%10);
tmp.setSeconds(0);
return y.format(tmp);
}


val JapanData = sc.textFile("data_1MB.csv").cache()


val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           (ArrondisDate(columns(0),simpleDateFormat),columns(1).substring(0,3),columns(4).toInt,columns(2).toFloat,columns(3).toFloat)
         })
       })
       


result.collect();

/*
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader


val result2 = JapanData.map{ line => val reader = new CSVReader(new StringReader(line),';');reader.readNext();}

val transform = result2.map{x=> (ArrondisDate(x(0),simpleDateFormat),x(1).substring(0,3),x(4).toInt,x(2).toFloat,x(3).toFloat)}

*/
