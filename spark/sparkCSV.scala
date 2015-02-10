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

/**
 * CREATE KEYSPACE test WITH replication = {
  'class': 'SimlpleStrategy',
  'Replication_factor': '2'
};

dse spark --executor-cores 2 --num-executors 4 --executor-memory 1G
dse spark --executor-cores 6 --driver-cores 4 --num-executors 5 --executor-memory 2G --driver-memory 1G 


sc.getConf.set("spark.cassandra.output.batch.size.bytes","1000000")

sc.getConf.set("spark.cassandra.output.concurrent.writes","10")


**/


//val JapanData = sc.textFile("s3n://bigdata-paristech/projet2014/data/data_100GB.csv ",5)
//val JapanData = sc.textFile("s3n://bigdata-paristech/projet2014/data/data_100GB.csv",200)

//Les temps indiqués le sont pour 1 GB sauf exception 

//Test avec nouvelle structure de table : ((t,id_ville),lat,longi,set(Tel))
//create table test_spark_set(t timestamp, id_ville text, lat float, longi float,tels set<int>, primary key ((t,id_ville),lat));
//1Go : 32 secondes de mapPartition  + 10 minutes
//pas réussi à intégrer avec les R2
val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3),columns(2),columns(3)),(columns(4)))
         })
       }).groupByKey(30).map(x=>(x._1._1,x._1._2,x._1._3,x._1._4,x._2)).saveToCassandra("test","test_spark_set")
  
  
//Commandes tests 
//create table test_spark_textTels(t timestamp, id_ville text,lat float,longi float,tel text, primary key((t,id_ville)));

//Test avec nouvelle structure de table : ((t,id_ville),lat,longi,concatenation text : 3 minutes +++
val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3),columns(2),columns(3)),(columns(4)+"|"))
         })   }).reduceByKey(_+_,24).map(x=>(x._1._1,x._1._2,x._1._3,x._1._4,x._2)).saveToCassandra("test","test_spark_texttels")
    

//Test avec nouvelle structure de table : ((t,id_ville),set(text : (lat+"/"longi+"/"+(Tel) 
//create table test_spark_unique_set(t timestamp, id_ville text, tels set<text>, primary key ((t,id_ville)));
//
//map : 1 minutes 
//cassandra : 3 minutes
val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3)),(columns(2)+"/"+columns(3)+"/"+columns(4)))
         })
       }).groupByKey(24).map(x=>(x._1._1,x._1._2,x._2)).saveToCassandra("test","test_spark_unique_set")
  

//Test avec nouvelle structure de table : ((t,id_ville),Text ( grosse concaténation=) :  
//create table test_spark_bigText(t timestamp, id_ville text, tels text, primary key ((t,id_ville)));
//map :  41 s
// cassandra : 48s
//~ 60 secondes
//10 Go : ~ 10 minutes 

//avec les nouvelles machines : 3 minutes

val result = JapanData.mapPartitions(lines => {
         val parser = new CSVParser(';')
         lines.map(line => {
           val columns = parser.parseLine(line)
           ((columns(0).substring(0,15)+"0:00",columns(1).substring(0,3)),(columns(2)+"/"+columns(3)+"/"+columns(4)+"|"))
         })
       }).reduceByKey(_ + _).map(x=>(x._1._1,x._1._2,x._2)).saveToCassandra("test","test_spark_bigtext")
  
