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
    
    public static final String SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" }"
            + "]}";
    

    

    
    private static void fillProperties(Properties props,String path){
       
        
        try(FileInputStream in = new FileInputStream(path)){
            props.load(in);
        }catch(IOException e){
            System.out.printf("Something went wrong when reading properties from %s \n",path);
        }

    }
    
    public static Properties getProperties(String... paths){
        
        Properties props = new Properties();
        for(String p:paths){
            if(p!=null){
                fillProperties(props, p);
            }
        }

        return props;
    }
    
}
