package com.github.flinkbwa;

import org.apache.log4j.BasicConfigurator;

/**
 * Main class to run FlinkBWA.
 *
 * Created by silvia on 10/04/19.
 */
public class FlinkBWA {
    /**
     * @param args Arguments from command line
     */
    public static void main(String[] args) {
        // To avoid log4j:WARN
        BasicConfigurator.configure();

        // Creation of BWAInterpreter
        BwaInterpreter newBwa = new BwaInterpreter(args);

        //Run of BWAInterpreter
        newBwa.runBwa();
    }
}