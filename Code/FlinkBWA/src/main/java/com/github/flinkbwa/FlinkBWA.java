package com.github.flinkbwa;

/**
 * Created by silvia on 11/04/19.
 */
public class FlinkBWA {
    /**
     * @param args Arguments from command line
     */
    public static void main(String[] args) {

        // Creation of BWAInterpreter
        BWAInterpreter newBwa = new BWAInterpreter(args);

        //Run of BWAInterpreter
        newBwa.runBwa();
    }
}