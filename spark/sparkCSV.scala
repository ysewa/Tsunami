import java.io.StringReader
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVParser

val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

def ArrondisDate(t:String,y:SimpleDateFormat):String ={
val tmp = y.parse(t);
val minutes= tmp.getMinutes();
tmp.setMinutes(minutes+10-minutes%10);
tmp.setSeconds(0);
return y.format(tmp);
}


val JapanData = sc.textFile("data_1MB.csv").cache()

def dropHeader(data: RDD[String]): RDD[String] = {
         data.mapPartitionsWithIndex((idx, lines) => {
           if (idx == 0) {
             lines.drop(1)
           }
           lines
         })
       }

val withoutHeader: RDD[String] = dropHeader(JapanData)

val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           (ArrondisDate(columns(0),simpleDateFormat),columns(1).substring(0,3),columns(4),columns(2),columns(3))
         })
       })
       


result.collect();

/*
import java.io.StringReader
import au.code.bytecode.opencsv.CSVReader


val result = JapanData.map{ line => val reader = new CSVReader(new StringReader(line));reader.readNext();}

val transform = result.map{x=> (ArrondisDate(x(0),simpleDateFormat),x(1).substring(0,3),x(4),x(2),x(3))}

*/
