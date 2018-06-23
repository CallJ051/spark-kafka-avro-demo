/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package myapp;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import static myapp.MyAvroSparkConsumer.execute;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author giuseppe.callari
 */
public class MyAvroSparkProducer {

    private static int MAX_MILLI_SECONDS = 5 * 1000;
    private static long SEED = 123;
    private static Schema.Parser parser = new Schema.Parser();
    private static Schema schema = parser.parse(Util.SCHEMA);
    private static Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
    private static  Producer<Long, byte[]> producer;
    private static Random rnd = new Random(SEED);
    
   
    //Main method for testing purposes
     public static void main(String... args) throws InterruptedException, IOException{
        execute(null, App.TRANSACTIONS,"test");
    }
    

    //based on https://www.tutorialkart.com/apache-spark/read-input-text-file-to-rdd-example/
    public static void execute(String pathProperties, String pathTransactions, String topic) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaSparkProducer")
                .setMaster("local[*]").set("spark.executor.memory", "2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // provide path to input text file
        // and read text file to RDD
        JavaRDD<String> lines = sc.textFile(pathTransactions);

        // get kafka producer
        producer= new KafkaProducer<>(Util.getProperties(App.PRODUCER_PROPERTIES,pathProperties));

        

        // collect RDD for printing
        lines.foreach(line -> {
            produceTransaction(line,topic);
        });

        producer.close();

    }

    public static void produceTransaction( String line,String topic) throws InterruptedException {
        Thread.sleep(rnd.nextInt(MAX_MILLI_SECONDS));//wait max MAX_MILLI_SECONDS milliseconds
        ZonedDateTime now = ZonedDateTime.now();//save transaction timestamp
        System.out.printf("Sending %s at %s  \n",line,DateTimeFormatter.ofPattern("hh:mm:ss").format(now));//Print transaction sent to kafka stream
        String[] splitted = line.split(",");//split transaction

        GenericData.Record avroRecord = new GenericData.Record(schema);
        
        //convert transaction line to avro record
        avroRecord.put(Util.DISTRIBUTOR_ID, splitted[0]);
        avroRecord.put(Util.POS_ID, splitted[1]);
        avroRecord.put(Util.VALUE, Double.valueOf(splitted[2]));

        producer.send(new ProducerRecord<Long, byte[]>(topic, now.toEpochSecond(), recordInjection.apply(avroRecord)));//send avro serialized transaction to kafka stream with timestamp as key
    }

}
