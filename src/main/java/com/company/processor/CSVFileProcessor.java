package com.company.processor;

import com.company.DBConnector;
import com.company.ExecutorFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

public class CSVFileProcessor implements FileProcessor {
    private static final Integer BATCH_SIZE = 4;
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
        BufferedReader br = null;
        FileInputStream inputStream = null;
        Scanner sc = null;
        String line = "";
        String cvsSplitBy = ",";
        long currentMills = System.currentTimeMillis();
        System.out.println("Start at = " + new Date());
        try {

            inputStream = new FileInputStream(filePath);
            sc = new Scanner(inputStream, "UTF-8");
            sc.useDelimiter(Pattern.compile("\"\\n"));
            FileReader reader = new FileReader(filePath);
            br = new BufferedReader(reader);
            List<String> lines = new ArrayList<>();
            int count=0;
            int tcount = 0;
            while (sc.hasNext()) {
                lines.add(sc.nextLine());
                count++;
                if(count == BATCH_SIZE){
                    executor.submit(new CSVWriter(new ArrayList(lines), cvsSplitBy, con));
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
            try {
                br.close();
                executor.shutdown();
                System.out.println("Executor shut down successfully");
                System.out.println(System.currentTimeMillis()-currentMills + "ms spent");
                System.out.println(ERROR_COUNT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
