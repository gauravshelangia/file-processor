package com.company.processor;

public interface FileProcessor {
    /**
     * Read file and save to DB
     * @param filePath
     */
    void saveFileToDB(String filePath);
}
