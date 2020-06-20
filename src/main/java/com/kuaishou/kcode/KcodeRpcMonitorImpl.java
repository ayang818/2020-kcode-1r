package com.kuaishou.kcode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
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
            e.printStackTrace();
        }
    }

    private void processBlock(byte[] block) {
        int lastLF = -1;
        int splitterTime = 0;
        int prePos = -1;
        boolean firstLine = true;
        String line;
        String[] dataArray = new String[7];
        for (int i = 0; i < block.length; i++) {
            byte bt = block[i];
            if (bt == 44) {
                dataArray[splitterTime] = new String(block, prePos + 1, i - prePos - 1);
                prePos = i;
                splitterTime += 1;
            }
            if (bt == 10) {
                // 处理完整行
                if (!firstLine) {
                    dataArray[splitterTime] = new String(block, prePos + 1, i - prePos - 1);
                } else {
                    lineBuilder.append(new String(block, 0, i));
                    line = lineBuilder.toString();
                    handleLine(line);
                    lineBuilder.delete(0, lineBuilder.length());
                    firstLine = false;
                }
                lastLF = i;
                splitterTime = 0;
                prePos = i;
            }
        }
        if (lastLF + 1 < block.length) {
            lineBuilder.append(new String(block, lastLF + 1, block.length - lastLF - 1));
        }
    }

    private void handleLine(String line) {
        String[] splitedData = line.split(",");
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


/* data format
imService31,10.190.246.247,openService4,10.163.127.219,true,202,1592301779942
imService31,10.101.62.175,openService4,10.92.133.81,true,115,1592301779419
imService31,10.225.89.100,openService4,10.240.25.1,true,266,1592301779276
imService31,10.246.232.55,openService4,10.209.111.231,true,263,1592301779529
imService31,10.101.62.175,openService4,10.209.111.231,true,108,1592301779134
imService31,10.230.150.71,openService4,10.240.25.1,true,140,1592301779159
imService31,10.246.232.55,openService4,10.163.127.219,true,243,1592301779005
imService31,10.101.62.175,openService4,10.163.127.219,true,239,1592301779305
imService31,10.200.19.91,openService4,10.165.188.14,true,144,1592301779440
imService31,10.83.192.112,openService4,10.240.25.1,true,107,1592301779553
 */