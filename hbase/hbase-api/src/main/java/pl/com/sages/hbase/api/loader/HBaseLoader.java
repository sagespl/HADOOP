package pl.com.sages.hbase.api.loader;

public abstract class HBaseLoader {

    final int COMMIT = 1000;

    public abstract void load();

}
