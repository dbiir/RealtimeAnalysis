package cn.edu.ruc.realtime.utils;

import java.io.*;

/**
 * Created by Jelly on 6/12/16.
 * Provide apis for writing to disk.
 */
public class Output {

    /**
     * Get a buffered writer
     * @param path file path to write
     * @param buffSize writer buffer size
     * */
    public static BufferedWriter getBufferedWriter(String path, int buffSize) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(getFileWriter(path), buffSize);
        return bufferedWriter;
    }

    /**
     * Get a file writer
     * @param path file path to write
     * */
    public static FileWriter getFileWriter(String path) throws IOException {
        FileWriter fWiter = new FileWriter(path);
        return fWiter;
    }

    /**
     * Get a buffered output stream
     * @param path file path to write
     * @param buffSize writer buffer size
     * */
    public static BufferedOutputStream getBufferedOutputStream(String path, int buffSize) throws IOException {
        return new BufferedOutputStream(new FileOutputStream(path), buffSize);
    }

    /**
     * Get a buffered writer. If file exists, append.
     * @param path file path to write
     * @param buffSize writer buffer size
     * */
    public static BufferedWriter getBufferedWriterAppend(String path, int buffSize) throws IOException {
        return new BufferedWriter(getFileWriterAppend(path), buffSize);
    }

    public static FileWriter getFileWriterAppend(String path) throws IOException {
        FileWriter fWriter;
        File f = new File(path);
        if (f.exists() && f.isFile()) {
            fWriter = new FileWriter(path, true);
        } else {
            fWriter = new FileWriter(path);
        }
        return fWriter;
    }
}
