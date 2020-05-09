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

import com.jcraft.jsch.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

public class SftpFileSystem extends FileSystem {
    private static final Log LOG = LogFactory.getLog(SftpFileSystem.class);
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;
    public static final String FS_SFTP_USER_PREFIX = "fs.sftp.user.";
    public static final String FS_SFTP_PASS_PREFIX = "fs.sftp.pass.";
    public static final String FS_SFTP_KEY_PREFIX = "fs.sftp.key.";
    public static final String FS_SFTP_HOST = "fs.sftp.host";
    public static final String FS_SFTP_HOST_PORT = "fs.sftp.host.port";
    public static final int SFTP_DEFAULT_PORT = 22;

    private URI uri;
    private Path homeDir;

    @Override
    public String getScheme() {
        return "sftp";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    protected int getDefaultPort() {
        return SFTP_DEFAULT_PORT;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        String host = uri.getHost();
        if (host == null) {
            host = conf.get(FS_SFTP_HOST);
        } else {
            conf.set(FS_SFTP_HOST, host);
        }
        if (host == null) {
            throw new IOException("Invalid host specified");
        }
        int port = uri.getPort();
        if (port < 0) {
            port = conf.getInt(FS_SFTP_HOST_PORT, SFTP_DEFAULT_PORT);
        }
        conf.setInt(FS_SFTP_HOST_PORT, port);
        String user = uri.getUserInfo();
        if (user != null) {
            conf.set(FS_SFTP_USER_PREFIX + host, user);
        }
        setConf(conf);
        this.uri = uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        try {
            connect();
            return new FSDataInputStream(new SftpInputStream(channel, getPathName(path), statistics));
        } catch (Exception e) {
            throw new IOException("open(): Failed to open the file " + path, e);
        }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        try {
            connect();
            Path workDir = new Path(channel.pwd());
            Path absolute = makeAbsolute(workDir, path);
            FileStatus status;
            try {
                status = getFileStatus(channel, path);
            } catch(IOException ioe) {
                status = null;
            }
            if (status != null) {
                if (overwrite && !status.isDirectory()) {
                    delete(channel, path, false);
                } else {
                    throw new FileAlreadyExistsException("File already exists: " + path);
                }
            }
            Path parent = absolute.getParent();
            if (parent == null || !mkdirs(channel, parent, FsPermission.getDirDefault())) {
                if (parent == null) {
                    throw new IOException("create(): mkdirs failed to create directory for " + path);
                }
            }
            OutputStream os = channel.put(getPathName(path));
            return new FSDataOutputStream(os, statistics);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        try {
            connect();
            Path workDir = new Path(channel.pwd());
            Path absolute = makeAbsolute(workDir, path);
            Path parent = absolute.getParent();
            if (parent == null || !mkdirs(channel, parent, FsPermission.getDirDefault())) {
                if (parent == null) {
                    throw new IOException("append(): mkdirs failed to create directory for " + path);
                }
            }
            OutputStream os = channel.put(getPathName(path), ChannelSftp.APPEND);
            return new FSDataOutputStream(os, statistics);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        connect();
        try {
            Path workDir = new Path(channel.pwd());
            Path absoluteSrc = makeAbsolute(workDir, src);
            Path absoluteDst = makeAbsolute(workDir, dst);
            channel.rename(absoluteSrc.getName(), absoluteDst.getName());
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        connect();
        return delete(channel, path, recursive);
    }


    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        connect();
        return listStatus(channel, f);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
    }

    @Override
    public Path getWorkingDirectory() {
        return getHomeDirectory();
    }

    @Override public Path getHomeDirectory() {
        return homeDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        connect();
        return mkdirs(channel, f, permission);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        connect();
        return getFileStatus(channel, f);
    }

    @Override
    public void close() throws IOException {
        disconnect();
        super.close();
    }

    private ChannelSftp channel;
    private Session session;
    private ChannelSftp connect() throws IOException {
        if (channel != null && channel.isConnected()) {
            try {
                channel = (ChannelSftp) session.openChannel("sftp");
                channel.connect(10000);
                return channel;
            } catch (JSchException e) {
            }
        }
        JSch jsch = new JSch();
        Configuration conf = getConf();
        String host = conf.get(FS_SFTP_HOST);
        int port = conf.getInt(FS_SFTP_HOST_PORT, SFTP_DEFAULT_PORT);
        String user = conf.get(FS_SFTP_USER_PREFIX + host);
        try {
            session = jsch.getSession(user, host, port);
            String pass = conf.get(FS_SFTP_PASS_PREFIX + host);
            String identityPath = conf.get(FS_SFTP_KEY_PREFIX + host);
            if (identityPath != null) {
                InputStream is = this.getClass().getClassLoader().getResourceAsStream(identityPath);
                if (is == null) {
                    File identityFile = new File(identityPath);
                    if (identityFile.isFile()) {
                        is = new FileInputStream(identityFile);
                    } else {
                        Path prvPath = new Path(identityPath);
                        FileSystem fs = FileSystem.get(conf);
                        is = fs.open(prvPath);
                    }
                }
                byte[] byteArray = new byte[10240];
                int size;
                int off = 0;
                while ((size = is.read(byteArray, off, 10240 - off)) >= 0) {
                    off += size;
                }
                is.close();
                byte[] prvKey = new byte[off];
                System.arraycopy(byteArray, 0, prvKey, 0, off);
                byte[] passphrase = pass == null? new byte[0]:pass.getBytes();
                jsch.addIdentity(user, prvKey, null, passphrase);
            } else {
                session.setPassword(pass);
            }
            Properties props = new Properties();
            props.put("StrictHostKeyChecking", "no");
            session.setConfig(props);
            session.connect(10000);
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(10000);
            homeDir = new Path(channel.pwd());
            return channel;
        } catch (JSchException | SftpException | IOException e) {
            throw new IOException("Login failed on server " + host, e);
        }
    }

    private void disconnect() {
        if (channel != null && channel.isConnected()) {
            channel.disconnect();
        }
        channel = null;
        if (session != null && session.isConnected()) {
            session.disconnect();
        }
        session = null;
    }

    private Path makeAbsolute(Path workDir, Path path) {
        return path.isAbsolute() ? path : new Path(workDir, path);
    }

    private String getPathName(Path path) {
        return path.toUri().getPath();
    }

    private FileStatus toFileStatus(SftpATTRS sftpAttrs, Path filePath) {
        long length = sftpAttrs.getSize();
        boolean isDir = sftpAttrs.isDir();
        long blockSize = DEFAULT_BLOCK_SIZE;
        long modTime = sftpAttrs.getMTime();
        long accessTime = sftpAttrs.getATime();
        FsPermission permission = new FsPermission((short) sftpAttrs.getPermissions());
        String user = Integer.toString(sftpAttrs.getUId());
        String group = Integer.toString(sftpAttrs.getGId());
        return new FileStatus(length, isDir, 1, blockSize, modTime, accessTime, permission, user, group, filePath);
    }

    private FileStatus getFileStatus(ChannelSftp channel, Path path) throws IOException {
        try {
            SftpATTRS sftpAttrs = channel.stat(getPathName(path));
            return toFileStatus(sftpAttrs, path);
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                throw new FileNotFoundException("File Not Found: " + path);
            } else {
                throw new IOException("getFileStatus(): Failed", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private FileStatus[] listStatus(ChannelSftp channel, Path path) throws IOException {
        try {
            Path workDir = new Path(channel.pwd());
            Path absolute = makeAbsolute(workDir, path);
            FileStatus fileStat = getFileStatus(channel, absolute);
            if (fileStat.isFile()) {
                return new FileStatus[]{fileStat};
            }
            Vector<ChannelSftp.LsEntry> lsEntries = channel.ls(absolute.toUri().getPath());
            List<FileStatus> fileStats = new ArrayList<>();
            for (ChannelSftp.LsEntry lsEntry: lsEntries) {
                String fileName = lsEntry.getFilename();
                if (!".".equals(fileName) && !"..".equals(fileName)) {
                    try {
                        fileStats.add(toFileStatus(lsEntry.getAttrs(), new Path(path, fileName)));
                    } catch (Exception e) {
                        LOG.info("Failed to open " + path + " " + fileName);
                    }
                }
            }
            return fileStats.toArray(new FileStatus[fileStats.size()]);
        } catch (SftpException e) {
            throw new IOException("listStatus(): unable to list status", e);
        }
    }

    private boolean delete(ChannelSftp channel, Path path, boolean recursive) throws IOException {
        try {
            Path workDir = new Path(channel.pwd());
            Path absolute = makeAbsolute(workDir, path);
            String pathName = absolute.toUri().getPath();
            try {
                FileStatus fileStat = getFileStatus(channel, absolute);
                if (fileStat.isFile()) {
                    channel.rm(pathName);
                    return true;
                }
            } catch (FileNotFoundException fnfe) {
                return false;
            }
            FileStatus[] dirEntries = listStatus(channel, absolute);
            if (dirEntries != null && dirEntries.length > 0 && !recursive) {
                throw new IOException("delete(): Directory " + path + " is not empty");
            }
            for (FileStatus dirEntry: dirEntries) {
                delete(channel, new Path(absolute, dirEntry.getPath()), recursive);
            }
            channel.rmdir(pathName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean exists(ChannelSftp channel, Path path) throws IOException {
        try {
            getFileStatus(channel, path);
            return true;
        } catch(FileNotFoundException e) {
            return false;
        }
    }

    private boolean isFile(ChannelSftp channel, Path path) {
        try {
            return getFileStatus(channel, path).isFile();
        } catch (IOException e) {
            return false;
        }
    }

    private boolean mkdirs(ChannelSftp channel, Path path, FsPermission permission) throws IOException {
        try {
            Path workDir = new Path(channel.pwd());
            Path absolute = makeAbsolute(workDir, path);
            String pathName = absolute.getName();
            if (!exists(channel, absolute)) {
                Path parent = absolute.getParent();
                if (parent != null) {
                    mkdirs(channel, parent, permission);
                    String parentDir = parent.toUri().getPath();
                    channel.cd(parentDir);
                } else {
                    channel.cd("/");
                }
                channel.mkdir(pathName);
            } else if (isFile(channel, absolute)) {
                throw new ParentNotDirectoryException("The path " + absolute + " is a file");
            }
            return true;
        } catch(SftpException e) {
            return false;
        }
    }
}
