package com.github.flinkbwa;

import org.apache.log4j.BasicConfigurator;

/**
 *
 * This class is used in case of need to use BWA from Java in a sequential way. Use: java -jar
 * BwaSeq.jar bwa mem ...
 *
 * Created by silvia on 10/04/19.
 */
public class BwaSeq {
    public static void main(String[] args) {
        // To avoid log4j:WARN
        BasicConfigurator.configure();

        BwaJni.Bwa_Jni(args);
    }
}
