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
package com.fuseinfo.spark.sql.sources.v2.cobol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

public class VariableLengthRecordReader extends RecordReader<LongWritable, BytesWritable> {

    private FSDataInputStream fileIn;
    private InputStream inputStream;
    private LongWritable key;
    private BytesWritable value;
    private long processed = 0;
    private long fileSize = 0;
    private boolean isCompressed;
    private Decompressor decompressor;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
        if (isCompressed = (null != codec)) {
            decompressor = CodecPool.getDecompressor(codec);
            inputStream = codec.createInputStream(fileIn, decompressor);
        } else {
            inputStream = fileIn;
        }
        fileSize = split.getLength();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }
        int high = inputStream.read();
        if (high < 0) {
            return false;
        }
        int low = inputStream.read();
        if (low < 0) {
            return false;
        }
        int size = (high << 8) + low;
        value.setSize(size);
        byte[] record = value.getBytes();
        int remaining = size;
        int offset = 0;
        while (remaining > 0) {
            int added = inputStream.read(record, offset, remaining);
            if (added == -1) {
                return false;
            }
            offset += added;
            remaining -= added;
        }
        processed += 2 + size;
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (fileSize == 0) {
            return 0.0f;
        }
        if (isCompressed) {
            return ((float)fileIn.getPos()) / fileSize;
        } else {
            return ((float)processed) / fileSize;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                decompressor = null;
            }
        }
    }
}
