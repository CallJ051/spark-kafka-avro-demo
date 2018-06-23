/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package myapp;

import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author giuseppe.callari
 */
public class App {
    //paths of test files
    public final static String TRANSACTIONS = "src/main/resources/transactions.csv";
    public final static String CHECK_POINT = "src/main/resources/checkpoint";
    public final static String RESULTS = "src/main/resources/myfile.txt"; 
    public final static String TOPIC = "test"; 
    
    
    public final static String APP = "spark-kafka-demo-app";
    public static String CONSUMER_PROPERTIES = "src/main/resources/consumer.properties"; //main consumer properties
    public static String PRODUCER_PROPERTIES = "src/main/resources/producer.properties"; //main producer properties

    private final static boolean TEST_MODE = false;

    public static void main(String... args) throws InterruptedException, IOException {

        CommandLine commandLine;
        HelpFormatter formatter = new HelpFormatter();
        Option optionCheckpoint = Option.builder("c")
                .required(false)
                .desc("The checkpoint option: path of directory used to save the consumer state")
                .longOpt("checkpoint")
                .hasArg() // This option has an argument.
                .build();
        Option optionType = Option.builder("s")
                .required(true)
                .desc("The stream type option - possible tyes: 'consumer','producer'")
                .longOpt("type")
                .hasArg() // This option has an argument.
                .build();
        Option optionResult = Option.builder("o")
                .required(false)
                .desc("The output option: path of file used to save the aggregations")
                .longOpt("output")
                .hasArg() 
                .build();

        Option optionTransactions = Option.builder("i")
                .required(false)
                .desc("The input option: path of file containing the transactions")
                .longOpt("input")
                .hasArg() // This option has an argument.
                .build();

        Option optionProperties = Option.builder("p")
                .required(false)
                .desc("The properties option: path of file containing additional streaming properties to overwrite default consumer or producer properties")
                .longOpt("properties")
                .hasArg() // This option has an argument.
                .build();
        
        Option optionTopic = Option.builder("t")
                .required(false)
                .desc("The topic option: a kafka topic")
                .longOpt("topic")
                .hasArg() // This option has an argument.
                .build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(optionCheckpoint);
        options.addOption(optionType);
        options.addOption(optionTransactions);
        options.addOption(optionResult);
        options.addOption(optionProperties);
        options.addOption(optionTopic);

        try {

            String[] testArgs = {"-o", RESULTS, "-c", CHECK_POINT, "-s", "producer", "-i", TRANSACTIONS,"-t",TOPIC};

            commandLine = parser.parse(options, TEST_MODE ? testArgs : args);

            if (commandLine.getOptionValue("s").equals("producer")) {

                if (!commandLine.hasOption("i") || !commandLine.hasOption("t")) {
                    throw new IllegalArgumentException("provide the path to the transaction file");
                }
                
                if(!commandLine.hasOption("t")) {
                    throw new IllegalArgumentException("provide the kafka topic");
                }
                MyAvroSparkProducer.execute(commandLine.getOptionValue("p"), commandLine.getOptionValue("i"),commandLine.getOptionValue("t"));
            } else if (commandLine.getOptionValue("s").equals("consumer")) {

                if (!commandLine.hasOption("o")) {
                    throw new IllegalArgumentException("provide the path to the results file");
                }
                if (!commandLine.hasOption("c")) {
                    throw new IllegalArgumentException("provide the path to the checkpoint directory");
                }
                if(!commandLine.hasOption("t")) {
                    throw new IllegalArgumentException("provide the kafka topic");
                }

                MyAvroSparkConsumer.execute(commandLine.getOptionValue("p"), commandLine.getOptionValue("o"), commandLine.getOptionValue("c"),commandLine.getOptionValue("t"));

            } else {
                throw new IllegalArgumentException("Use a correct stream type and corresponding arguments");
            }

        } catch (ParseException exception) {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());

            formatter.printHelp(APP, options);
        } catch (IllegalArgumentException e) {
            System.out.print("Argument error: ");
            System.out.println(e.getMessage());
            formatter.printHelp(APP, options);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
            System.out.println("Could not find necessary serialization classes");
        }
    }
}
