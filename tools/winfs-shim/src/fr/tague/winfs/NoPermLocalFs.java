package fr.tague.winfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFs;
import org.apache.hadoop.fs.DelegateToFileSystem;

/** Version FileContext (utilisée par les checkpoints Structured Streaming). */
public class NoPermLocalFs extends ChecksumFs {
    public NoPermLocalFs(URI uri, Configuration conf) throws IOException, URISyntaxException {
        super(new Raw(uri, conf));
    }

    static class Raw extends DelegateToFileSystem {
        Raw(URI uri, Configuration conf) throws IOException, URISyntaxException {
            super(uri, new NoPermRawFileSystem(), conf, "file", false);
        }
        @Override public int getUriDefaultPort() { return -1; }
    }
}
