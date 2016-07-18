package cn.edu.ruc.realtime.model;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class Meta {

    private int fiberId;
    private long beginTime;
    private long endTime;
    private String filename;

    public int getFiberId() {
        return fiberId;
    }

    public void setFiberId(int fiberId) {
        this.fiberId = fiberId;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
