package cn.edu.ruc.realtime.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Jelly on 6/12/16.
 * Logger
 */
public class Log {
    private String name;
    private BufferedWriter logWriter;
    private String logBase = "./";              // log base dir, default ot './'
    private String timestamp;

    public Log(String name) {
        this.name = name;
        try {
            logBase = ConfigFactory.getInstance().getProps("log.dir").trim();
            // check logBase to fit in LINUX or UNIX file directory naming convention.
            if (!logBase.endsWith("/")) {
                logBase += "/";
            }
            logWriter = Output.getBufferedWriterAppend(logBase + name, 1024*1);
        } catch (IOException e) {
            // TODO error handler
            e.printStackTrace();
        }
    }

    public String getName() {
        return this.name;
    }

    /**
     * Log error message
     * @param msg error message
     * */
    public synchronized void error(String msg) {
        try {
            timestamp = new SimpleDateFormat("yyyy-mm-dd:HH-mm-ss").format(new Date());
            logWriter.write(timestamp + " [ERROR] " + msg + "\n");
            logWriter.flush();
        } catch (IOException ioe) {
            // TODO error handler
            ioe.printStackTrace();
        }
    }

    /**
     * Log error message
     * @param err Error
     * */
    public synchronized void error(Error err) {
        try {
            timestamp = new SimpleDateFormat("yyyy-mm-dd:HH-mm-ss").format(new Date());
            String errMsg = timestamp + "[ERROR] " + err.getMessage() + "\n";
            for (StackTraceElement element : err.getStackTrace()) {
                errMsg += "\t[Error Stack] " + element.getClassName() + "." + element.getMethodName() +
                        " at " + element.getFileName() + ":" + element.getLineNumber() + "\n";
            }
            logWriter.write(errMsg);
            logWriter.flush();
        } catch (IOException ioe) {
            // TODO error handler
            ioe.printStackTrace();
        }
    }

    /**
     * Log exception message
     * @param msg exception message
     * */
    public synchronized void exception(String msg) {
       try {
           timestamp = new SimpleDateFormat("yyyy-mm-dd:HH-mm-ss").format(new Date());
           logWriter.write(timestamp + " [EXCEPTION] " + msg + "\n");
           logWriter.flush();
       } catch (IOException ioe) {
           // TODO error handler
           ioe.printStackTrace();
       }
    }

    /**
     * Log exception message
     * @param e Exception
     * */
    public synchronized void exception(Exception e) {
        try {
            timestamp = new SimpleDateFormat("yyyy-mm-dd:HH-mm-ss").format(new Date());
            String expMsg = timestamp + " [EXCEPTION] " + e.getMessage() + "\n";
            for (StackTraceElement element : e.getStackTrace()) {
                expMsg += "\t[EXCEPTION MSG] " + element.getClassName() + "."  + element.getMethodName()
                        + " at " + element.getFileName() + ":" + element.getLineNumber() + "\n";
            }
            logWriter.write(expMsg);
            logWriter.flush();
        } catch (IOException ioe) {
            // TODO err handler
            ioe.printStackTrace();
        }
    }

    /**
     * Log warning message
     * @param msg warning message
     * */
    public synchronized void warn(String msg) {
        try {
            timestamp = new SimpleDateFormat("yyyy-mm-dd:HH-mm-ss").format(new Date());
            logWriter.write(timestamp + " [WARN] " + msg + "\n");
            logWriter.flush();
        } catch (IOException ioe) {
            // TODO error handler
            ioe.printStackTrace();
        }
    }

    /**
     * Log info message
     * @param msg info message
     * */
    public synchronized void info(String msg) {
        try {
            timestamp = new SimpleDateFormat("yyyy-mm-dd:HH-mm-ss").format(new Date());
            logWriter.write(timestamp + " [INFO] " + msg + "\n");
            logWriter.flush();
        } catch (IOException ioe) {
            // TODO error handler
            ioe.printStackTrace();
        }
    }
}
