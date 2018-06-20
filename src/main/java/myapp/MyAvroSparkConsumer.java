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

public class MyAvroSparkConsumer {
    
    static {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(Util.SCHEMA);
            recordInjection = GenericAvroCodecs.toBinary(schema);
        }
    public static Injection<GenericRecord, byte[]> recordInjection;
    public static String CONSUMER_PROPERTIES="src/main/resources/consumer.properties";
    
    //Main method for testing purposes
    public static void main(String... args) throws InterruptedException, IOException{
        execute(null, App.RESULTS, App.CHECK_POINT);
    }
    
    public static void execute(String pathProperties,String pathOutputFile,String pathCheckPoint) throws InterruptedException, IOException {

        Collection<String> topics = Arrays.asList("test");
        final File outputFile = new File(pathOutputFile);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaDirectKafkaWordCount")
                .setMaster("local[*]");

        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(pathCheckPoint, ()
                -> {
            JavaStreamingContext jc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

            jc.checkpoint("src/main/resources/checkpoint");//needed for updatestates; windows dependent

            final JavaInputDStream<ConsumerRecord<Long, byte[]>> stream = KafkaUtils.createDirectStream(
                    jc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<Long, byte[]>Subscribe(topics, (Map) Util.getProperties(CONSUMER_PROPERTIES,pathProperties))
            );

            JavaDStream<RIATransaction> txs = stream
                    .map(x -> new RIATransaction(x.key(), x.value())); //use avro

            txs.foreachRDD(x -> {
                System.out.println("======== NEW TX =========");
                x.collect().forEach(System.out::println);
                System.out.println("=========================");
            });

            txs.mapToPair(x -> new Tuple2<String, Tuple2<Number, Number>>(x.distributorId, new Tuple2<>(x.value, 0)))
                    
                    .reduceByKey((x, y) -> new Tuple2<>(x._1.doubleValue() + y._1.doubleValue(), y._1.doubleValue() / x._1.doubleValue()))
                    
                    .updateStateByKey((List<Tuple2<Number, Number>> l, Optional<Tuple2<Number, Number>> y) -> {
                        Tuple2<Number, Number> sum = y.or(new Tuple2<Number, Number>(0, 0));
                        double dsum = sum._1.doubleValue();
                        for (Tuple2<Number, Number> t : l) {
                            dsum += t._1.doubleValue();
                        }
                        return Optional.of(new Tuple2<Number, Number>(dsum, (dsum - sum._1.doubleValue()) / (dsum == 0 ? 1 : dsum)));
                    })
                    .foreachRDD(x -> writeToFile(outputFile, x)
                    );

            return jc;
        });
        http://teknosrc.com/spark-error-java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-hadoop-binaries/

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

    public static void writeToFile(File f, JavaPairRDD<String, Tuple2<Number, Number>> t) {
        try {

            String toWrite = t.collectAsMap().entrySet().stream()
                    .map(x -> String.format("%d,%s,%f,%f \n", t.id(), x.getKey(), x.getValue()._1, x.getValue()._2)) //use time last transaction
                    .reduce("", (tot, v) -> tot + v);

            System.out.printf("Reduction:\n %s \n", toWrite);

            Files.append(toWrite, f, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

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

}
