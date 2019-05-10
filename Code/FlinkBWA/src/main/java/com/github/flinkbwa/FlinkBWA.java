package com.github.flinkbwa;

import org.apache.log4j.BasicConfigurator;

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
