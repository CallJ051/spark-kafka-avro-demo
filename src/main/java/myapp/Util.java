/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package myapp;

import com.google.common.base.Predicates;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

/**
 *
 * @author giuseppe.callari
 */
public class Util {
    
    
    public static String DISTRIBUTOR_ID = "DISTRIBUTOR_ID";
    public static String POS_ID= "POS_ID";
    public static String VALUE = "VALUE";
    public static final String SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\""+DISTRIBUTOR_ID+"\", \"type\":\"string\" },"
            + "  { \"name\":\""+POS_ID+"\", \"type\":\"string\" },"
            + "  { \"name\":\""+VALUE+"\", \"type\":\"double\" }"
            + "]}";
    

    

    
    private static void fillProperties(Properties props,String path){
       
        
        try(FileInputStream in = new FileInputStream(path)){
            props.load(in);
        }catch(IOException e){
            System.out.printf("Something went wrong when reading properties from %s \n",path);
        }

    }
    
    public static Properties getProperties(String... paths){ //get properties 
        
        Properties props = new Properties();
        for(String p:paths){
            if(p!=null){
                fillProperties(props, p);
            }
        }

        return props;
    }
    
}
