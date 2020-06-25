package com.company;

import com.company.processor.CSVFileProcessor;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        System.out.println( "Hello World!" );
        String csvFilePath = "conf/products.csv";
        CSVFileProcessor csvFileProcessor = new CSVFileProcessor();
        csvFileProcessor.saveFileToDB(csvFilePath);
    }
}
