package com.github.flinkbwa;
/**
 *
 * This class is used in case of need to use BWA from Java in a sequential way. Use: java -jar
 * BwaSeq.jar bwa mem ...
 *
 * Created by silvia on 10/04/19.
 */
public class BwaSeq {
    public static void main(String[] args) {
        BwaJni.Bwa_Jni(args);
    }
}
