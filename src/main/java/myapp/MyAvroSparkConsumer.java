/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package myapp;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.api.java.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.spark_project.guava.io.Files;
import scala.Tuple2;
import org.apache.avro.util.Utf8;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import static scala.Predef.classOf;

public class MyAvroSparkConsumer {

    static {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(Util.SCHEMA);
        recordInjection = GenericAvroCodecs.toBinary(schema);//initialize converter
    }
    public static Injection<GenericRecord, byte[]> recordInjection;
   

    //Main method for testing purposes
    public static void main(String... args) throws InterruptedException, IOException, ClassNotFoundException {
        execute(null, App.RESULTS, App.CHECK_POINT,App.TOPIC);
    }

    // Install hadoop winutils when using windows and add it to system's class path  
    //  http://teknosrc.com/spark-error-java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-hadoop-binaries/
    public static void execute(String pathProperties, String pathOutputFile, String pathCheckPoint,String topic) throws InterruptedException, IOException, ClassNotFoundException {

        Collection<String> topics = Arrays.asList(topic);
        final File outputFile = new File(pathOutputFile);

//        if (outputFile.exists()) { 
//            outputFile.delete(); //for testing purposes: delete if file already exists 
//        }
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaSparkConsumer")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //needed when using injection API
                .registerKryoClasses(new Class<?>[]{Class.forName("org.apache.avro.generic.GenericData$Record")});

        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(pathCheckPoint, () //get saved or create new JavaStreamingContext
                -> {

            JavaStreamingContext jc = new JavaStreamingContext(sparkConf, Durations.seconds(3));//3 second window

            jc.checkpoint(pathCheckPoint);//needed for persistence of application (consumer) state; windows dependent

            
            //Create Spark stream using Kafka stream as src
            final JavaInputDStream<ConsumerRecord<Long, byte[]>> stream = KafkaUtils.createDirectStream(
                    jc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<Long, byte[]>Subscribe(topics, (Map) Util.getProperties(App.CONSUMER_PROPERTIES, pathProperties)) //get consumer properties
            );

            
            // mapping to generic avro records
            JavaDStream<Tuple2<Long, GenericRecord>> txs = stream
                    .map(x -> new Tuple2<>(x.key(), recordInjection.invert(x.value()).get())); //map byte array to avro generic record

            //print incoming transactions
            txs.foreachRDD(x -> {
                System.out.println("======== NEW TX ========="); //print new incoming transactions for testing purposes
                x.collect().forEach(y -> {
                    System.out.printf("[%s] Distributor ID: %s \t  pos ID: %s \t value: %f \n",
                            LocalDateTime.ofEpochSecond(y._1, 0, ZoneOffset.UTC).toString(),
                            y._2.get(Util.DISTRIBUTOR_ID),
                            y._2.get(Util.POS_ID),
                            y._2.get(Util.VALUE));
                });
                System.out.println("=========================");
            });

            //map avro records to pairs in order to reduce and calculate accumulations over the Distributor ID 
            txs.mapToPair(x -> new Tuple2<String, Tuple2<Double, Double>>(((Utf8) x._2.get(Util.DISTRIBUTOR_ID)).toString(), new Tuple2<>((double) x._2.get(Util.VALUE), 0.0))) //convert avro record to pair 

                    .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, y._1 / x._1)) //reduction per key

                    .updateStateByKey((List<Tuple2<Double, Double>> l, Optional<Tuple2<Double, Double>> y) -> {  //accumulation per key and delta calculation (%)
                        Tuple2<Double, Double> sum = y.or(new Tuple2<>(0.0, 0.0));
                        double dsum = sum._1.doubleValue();
                        for (Tuple2<Double, Double> t : l) {
                            dsum += t._1;
                        }
                        return Optional.of(new Tuple2<>(
                                dsum, //accumulation
                                (dsum - sum._1) / (dsum == 0 ? 1 : dsum)) //delta (%)
                        );
                    })
                    
                    
                    .foreachRDD(x -> writeToFile(outputFile, x));// write accumulations to file
                    

            return jc;
        });

        // Start the computation
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

    public static void writeToFile(File f, JavaPairRDD<String, Tuple2<Double, Double>> t) {
        try {

            String toWrite = t.collectAsMap().entrySet().stream()
                    .map(x -> String.format(Locale.ROOT, "%d,%s,%.2f,%.2f \n", t.id(), x.getKey(), x.getValue()._1, 100 * x.getValue()._2)) //use time last transaction
                    .reduce("", (tot, v) -> tot + v);

            System.out.printf("Reduction:\n%s \n", toWrite);
            if (f.length() == 0) {
                Files.append("rdd_id,key,sum,delta %\n", f, Charset.defaultCharset()); //create header
            }
            Files.append(toWrite, f, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    /* OVERKILL
    public static class RIATransaction implements Serializable {//TODO time?

    
        private String distributorId;
        private String posId;
        private Number value;
        private String key;
        private String raw;

        public String toString() {
            return String.format("[%s] Distributor ID: %s \t  pos ID: %s \t value: %d", key, distributorId, posId, value);
        }

        public RIATransaction(String key, String raw) {
            String[] splitted = raw.split(",");
            this.distributorId = splitted[0];
            this.posId = posId = splitted[1];
            this.value = new Integer(splitted[2]);
            this.raw = raw;
            this.key = key;
        }

        public RIATransaction(String key, byte[] raw) {

            GenericRecord record = recordInjection.invert(raw).get();

            this.distributorId = ((Utf8) record.get("str1")).toString();
            this.posId = ((Utf8) record.get("str2")).toString();
            this.value = (int) record.get("int1");
            this.raw = record.toString();
            this.key = key;
        }

        public RIATransaction(long key, byte[] raw) {
            this(LocalDateTime.ofEpochSecond(key, 0, ZoneOffset.UTC).toString(), raw);
        }

    }
     */

}
