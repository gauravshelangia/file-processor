package com.company.processor;

import com.company.DBConnector;
import com.company.ExecutorFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

public class CSVFileProcessor implements FileProcessor {
    private static final Integer BATCH_SIZE = 10;
    public static Integer ERROR_COUNT = 1;
    Connection con;
    ExecutorService executor;

    public CSVFileProcessor() {
        // Use one executor in one processor
        con = DBConnector.getConnection();
        // Number of threads should be configurable -- read from env
        executor = ExecutorFactory.getExecutorService(1);
    }

    @Override
    public void saveFileToDB(String filePath) {
        String cvsSplitBy = ",";
        long currentMills = System.currentTimeMillis();
        System.out.println("Start at = " + new Date());
        try {
            List<String[]> lines = new ArrayList<>();
            int count=0;
            int tcount = 0;

            CSVParser csvParser = new CSVParser(new FileReader(filePath),
                    CSVFormat.DEFAULT.withFirstRecordAsHeader());
            Iterator<CSVRecord> it = csvParser.iterator();

            while(it.hasNext()) {
                CSVRecord csvRecord = it.next();
                lines.add(new String[]{csvRecord.get(0),csvRecord.get(1),csvRecord.get(2)});
                count++;
                if(count == BATCH_SIZE){
                    executor.submit(new CSVWriter(lines, cvsSplitBy, con));
                    count = 0;
                    lines = new ArrayList<>();
                }
                if(tcount%10000==0){
                    System.out.println("T Count = " + tcount + " At = " + new Date());
                }
                tcount++;
            }
            if (count>0){
                executor.submit(new CSVWriter(lines, cvsSplitBy, con));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
            System.out.println("Executor shut down successfully");
            System.out.println(System.currentTimeMillis()-currentMills + "ms spent");
            System.out.println(ERROR_COUNT);
        }
    }


}
