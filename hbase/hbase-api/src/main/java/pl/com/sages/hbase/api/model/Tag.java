package pl.com.sages.hbase.api.model;

public class Tag {

    private int userId;
    private int movieId;
    private String tag;
    private long timestamp;

    public Tag(int userId, int movieId, String tag, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.tag = tag;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Tag{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", tag='" + tag + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
