package com.github.flinkbwa;

public class FlinkBWA {
    /**
     * @param args Arguments from command line
     */
    public static void main(String[] args) {

        // Creation of BWAInterpreter
        BwaInterpreter newBwa = new BwaInterpreter(args);

        //Run of BWAInterpreter
        newBwa.runBwa();
    }
}
