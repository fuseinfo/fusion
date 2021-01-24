package org.apache.hadoop.fs.smb;

import jcifs.smb.SmbRandomAccessFile;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class SmbInputStream extends FSInputStream {
    private FileSystem.Statistics stats;
    private SmbRandomAccessFile raf;

    public SmbInputStream(SmbRandomAccessFile raf, FileSystem.Statistics stats) {
        this.raf = raf;
        this.stats = stats;
    }

    @Override
    public void seek(long pos) throws IOException {
        raf.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return raf.getFilePointer();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        int byteRead = raf.read();
        if (stats != null && byteRead >= 0) {
            stats.incrementBytesRead(1);
        }
        return byteRead;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        int nRead = raf.read(buf, off, len);
        if (stats != null && nRead > 0) {
            stats.incrementBytesRead(nRead);
        }
        return nRead;
    }

    @Override
    public void close() throws IOException {
        raf.close();
        super.close();
    }
}
