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
/**
 * CREATE KEYSPACE test WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'Analytics': '5'
};
**/


//val JapanData = sc.textFile("data_1MB.csv",5).cache()

//Les temps indiqués le sont pour 1 GB sauf exception 

//Test avec nouvelle structure de table : ((t,id_ville),lat,longi,set(Tel))
val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3),columns(2),columns(3)),(columns(4)))
         })
       }).groupByKey(10).map(x=>(x._1._1,x._1._2,x._1._3,x._1._4,x._2)).saveToCassandra("test","test_spark_set")
  
  //Commandes tests 
//Test avec nouvelle structure de table : ((t,id_ville),lat,longi,concatenation text : 3 minutes

val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3),columns(2),columns(3)),(columns(4)))
         })
       }).reduceByKey(_+_,10).map(x=>(x._1._1,x._1._2,x._1._3,x._1._4,x._2)).saveToCassandra("test","test_spark_texttels")
  

//Test avec nouvelle structure de table : ((t,id_ville),set(text : (lat+"/"longi+"/"+(Tel) 5,7 minutes
val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3)),(columns(2)+"/"+columns(3)+"/"+columns(4)))
         })
       }).groupByKey(10).map(x=>(x._1._1,x._1._2,x._2)).saveToCassandra("test","test_spark_unique_set")
  

//Test avec nouvelle structure de table : ((t,id_ville),Text ( grosse concaténation=) :  ~ 60 secondes
//10 Go : ~ 10 minutes 
val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3)),(columns(2)+"/"+columns(3)+"/"+columns(4)+"|"))
         })
       }).reduceByKey(_ + _,10).map(x=>(x._1._1,x._1._2,x._2)).saveToCassandra("test","test_spark_bigtext")
  

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
