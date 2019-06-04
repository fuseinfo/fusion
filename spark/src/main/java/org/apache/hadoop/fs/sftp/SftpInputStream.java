/*
 * Copyright (c) 2018 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.apache.hadoop.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;

public class SftpInputStream extends FSInputStream {
    private String src;
    private InputStream wrappedStream;
    private ChannelSftp channel;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long pos;

    SftpInputStream(ChannelSftp channel, String pathName, FileSystem.Statistics stats) {
        if (channel == null || !channel.isConnected()) {
            throw new IllegalArgumentException("Sftp channel null or not connected");
        }
        try {
            wrappedStream = channel.get(pathName);
            this.src = pathName;
            this.channel = channel;
            this.stats = stats;
            this.pos = 0;
            this.closed = false;
        } catch (SftpException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        try {
            wrappedStream = channel.get(src, null, pos);
            this.pos = pos;
        } catch (SftpException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws  IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        int byteRead = wrappedStream.read();
        if (byteRead >= 0) {
            pos++;
        }
        if (stats != null && byteRead >= 0) {
            stats.incrementBytesRead(1);
        }
        return byteRead;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("System closed");
        }
        int result = wrappedStream.read(buf, off, len);
        if (result > 0) {
            pos += result;
        }
        if (stats != null && result > 0) {
            stats.incrementBytesRead(result);
        }
        return result;
    }

    @Override
    public synchronized  void close() throws IOException {
        try {
            wrappedStream.close();
        } catch (Exception e) {
        }
        if (closed) {
            return;
        }
        super.close();
        closed = true;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }

}
