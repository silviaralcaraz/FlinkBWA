package com.github.flinkbwa;

import cz.adamh.utils.NativeUtils;
import java.io.IOException;

/**
 * Class that calls BWA functions by means of JNI
 */
public class BwaJni {
    static {
        try {
            NativeUtils.loadLibraryFromJar("/libbwa.so");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function to call BWA native main method from Java
     * @param args A String array with the arguments to call BWA
     * @return The BWA integer result value
     */
    public static int Bwa_Jni(String[] args) {
        int[] lenStrings = new int[args.length];
        int i = 0;
        for (String arg : args) {
            lenStrings[i] = arg.length();
        }
        int returnCode = new BwaJni().bwa_jni(args.length, args, lenStrings);
        return returnCode;
    }

    //Declaration of native method
    private native int bwa_jni(int argc, String[] argv, int[] lenStrings);
}