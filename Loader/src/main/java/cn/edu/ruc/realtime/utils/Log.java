//package cn.edu.ruc.realtime.utils;
//
//import java.io.BufferedWriter;
//import java.io.IOException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//
///**
// * Created by Jelly on 6/12/16.
// * Logger
// */
//public class Log {
//    private String name;
//    private BufferedWriter logWriter;
//    private String logBase = "./";              // log base dir, default ot './'
//    private String timestamp;
//
//    public Log(String name) {
//        this.name = name;
//        try {
//            logBase = ConfigFactory.getInstance().getLogDir();
//            // check logBase to fit in LINUX or UNIX file directory naming convention.
//            if (!logBase.endsWith("/")) {
//                logBase += "/";
//            }
//            logWriter = Output.getBufferedWriterAppend(logBase + name, 1024*1);
//        } catch (IOException e) {
//            // TODO error handler
//            e.printStackTrace();
//        }
//    }
//
//    public String getName() {
//        return this.name;
//    }
//
//    /**
//     * Log error message
//     * @param msg error message
//     * */
//    public void error(String msg) {
//        logMsg(msg, "error");
//    }
//
//    /**
//     * Log error message
//     * @param err Error
//     * */
//    public void error(Error err) {
//        logThw(err);
//    }
//
//    /**
//     * Log exception message
//     * @param msg exception message
//     * */
//    public void exception(String msg) {
//        logMsg(msg, "exception");
//    }
//
//    /**
//     * Log exception message
//     * @param e Exception
//     * */
//    public void exception(Exception e) {
//        logThw(e);
//    }
//
//    /**
//     * Log warning message
//     * @param msg warning message
//     * */
//    public void warn(String msg) {
//        logMsg(msg, "warn");
//    }
//
//    /**
//     * Log info message
//     * @param msg info message
//     * */
//    public void info(String msg) {
//        logMsg(msg, "info");
//    }
//
//    /**
//     * Log message
//     * @param msg log message
//     * @param type log type: info, warn, exception, error
//     * @synchronized
//     * */
//    private synchronized void logMsg(String msg, String type) {
//        try {
//            timestamp = new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss z").format(new Date());
//            logWriter.write(timestamp + "[" + type + "]" + msg + "\n");
//            logWriter.flush();
//        } catch (IOException ioe) {
//            ioe.printStackTrace();
//        }
//    }
//
//    /**
//     * Log throwable
//     * @param t throwable
//     * @synchronized
//     * */
//    private synchronized void logThw(Throwable t) {
//        String type;
//        if (t.getClass() == Exception.class) {
//            type = "EXCEPTION";
//        } else {
//            type = "ERROR";
//        }
//        try {
//            timestamp = new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss z").format(new Date());
//            String expMsg = timestamp + " [" + type + "] " + t.getMessage() + "\n";
//            for (StackTraceElement element : t.getStackTrace()) {
//                expMsg += "\t[" + type + " MSG] " + element.getClassName() + "."  + element.getMethodName()
//                        + " at " + element.getFileName() + ":" + element.getLineNumber() + "\n";
//            }
//            logWriter.write(expMsg);
//            logWriter.flush();
//        } catch (IOException ioe) {
//            // TODO err handler
//            ioe.printStackTrace();
//        }
//    }
//}
