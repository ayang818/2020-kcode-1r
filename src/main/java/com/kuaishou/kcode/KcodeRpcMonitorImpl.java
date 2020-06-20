package com.kuaishou.kcode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kcode
 * Created on 2020-06-01
 * 实际提交时请维持包名和类名不变
 */

public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {

    StringBuilder lineBuilder = new StringBuilder();

    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
    }

    @Override
    public void prepare(String path) {
        try {
            RandomAccessFile memoryMappedFile = new RandomAccessFile(path, "rw");
            FileChannel channel = memoryMappedFile.getChannel();
            // try to use 16KB buffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 16);
            byte[] bts;
            while (channel.read(byteBuffer) != -1) {
                byteBuffer.flip();
                int remain = byteBuffer.remaining();
                bts = new byte[remain];
                byteBuffer.get(bts, 0, remain);
                byteBuffer.clear();
                processBlock(bts);
            }
        } catch (IOException e) {

        }
    }

    private void processBlock(byte[] block) {
        int lastLF = -1;
        String line;
        boolean firstLine = true;
        for (int i = 0; i < block.length; i++) {
            byte bt = block[i];
            if (bt == 10) {
                if (!firstLine) {
                    line = new String(block, lastLF + 1, i - lastLF - 1);
                } else {
                    lineBuilder.append(new String(block, 0, i));
                    line = lineBuilder.toString();
                    lineBuilder.delete(0, lineBuilder.length());
                    firstLine = false;
                }
                lastLF = i;

            }
        }
        if (lastLF + 1 < block.length) {
            lineBuilder.append(new String(block, lastLF + 1, block.length));
        }
    }

    @Override
    public List<String> checkPair(String caller, String responder, String time) {
        return new ArrayList<String>();
    }

    @Override
    public String checkResponder(String responder, String start, String end) {
        return "0.00%";
    }

}
