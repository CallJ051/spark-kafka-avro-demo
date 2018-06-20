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

    public final static String TRANSACTIONS = "src/main/resources/transactions.csv";
    public final static String CHECK_POINT = "src/main/resources/checkpoint";
    public final static String RESULTS = "src/main/resources/myfile.txt";
    public final static String APP = "spark-kafka-demo-app";
    private final static boolean TEST_MODE = false;

    public static void main(String... args) throws InterruptedException, IOException {

        CommandLine commandLine;
        HelpFormatter formatter = new HelpFormatter();
        Option optionCheckpoint = Option.builder("c")
                .required(false)
                .desc("The checkpoint option: path of directory used to save the consumer state")
                .longOpt("checkpoint")
                .build();
        Option optionType = Option.builder("s")
                .required(true)
                .desc("The stream type option - possible tyes: 'consumer','producer'")
                .longOpt("type")
                .build();
        Option optionResult = Option.builder("r")
                .required(false)
                .desc("The result option: path of file used to save the aggregations")
                .longOpt("result")
                .build();

        Option optionTransactions = Option.builder("t")
                .required(false)
                .desc("The transactions option: path of file containing the transactions")
                .longOpt("transations")
                .build();

        Option optionProperties = Option.builder("p")
                .required(false)
                .desc("The transactions option: path of file containing streaming properties")
                .longOpt("properties")
                .build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(optionCheckpoint);
        options.addOption(optionType);
        options.addOption(optionTransactions);
        options.addOption(optionResult);
        options.addOption(optionProperties);

        try {

            String[] testArgs = {"-r", RESULTS, "-c", CHECK_POINT, "-s", "producer", "-t", TRANSACTIONS};

            commandLine = parser.parse(options, TEST_MODE ? testArgs : args);

            

            if (commandLine.getOptionValue("s").equals("producer")) {

                if (!commandLine.hasOption("t")) {
                    throw new IllegalArgumentException("provide the path to the transaction file");
                }
                MyAvroSparkProducer.execute(commandLine.getOptionValue("p"), commandLine.getOptionValue("t"));
            } else if (commandLine.getOptionValue("s").equals("consumer")) {

                if (!commandLine.hasOption("r")) {
                    throw new IllegalArgumentException("provide the path to the results file");
                }
                if (!commandLine.hasOption("c")) {
                    throw new IllegalArgumentException("provide the path to the checkpoint directory");
                }

                MyAvroSparkConsumer.execute(commandLine.getOptionValue("p"), commandLine.getOptionValue("r"), commandLine.getOptionValue("c"));

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
        }
    }
}
