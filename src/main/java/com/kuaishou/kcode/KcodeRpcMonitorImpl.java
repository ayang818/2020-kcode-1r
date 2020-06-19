package com.kuaishou.kcode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
    }

    @Override
    public void prepare(String path) {
        try {
            FileInputStream fileInputStream = new FileInputStream(new File(path));
            FileChannel channel = fileInputStream.getChannel();
            // try to use 4KB buffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 4);
            byte[] bts;
            while (channel.read(byteBuffer) != -1) {
                byteBuffer.flip();
                int remain = byteBuffer.remaining();
                bts = new byte[remain];
                byteBuffer.get(bts, 0, remain);
                byteBuffer.clear();

                
            }
        } catch(IOException e) {

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
