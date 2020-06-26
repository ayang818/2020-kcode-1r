package com.kuaishou.kcode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// TODO 寻找边界情况和可能出现问题的代码吧
/**
 * @author kcode
 * Created on 2020-06-01
 * 实际提交时请维持包名和类名不变
 */
public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {
    StringBuilder lineBuilder = new StringBuilder();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    // 数据的所有特点 servicePair少；timestamp极少，代表每分钟；ipPair也很少，集中在30左右；多的就是调用次数
    // 查询1数据结构
    // Map<(caller, responder), Map<timestamp, Map<(callerIp, responderIp), Object(heap[costTime...costTime], sucTime, totalTime)>>>
    Map<String, Map<Long, Map<String, Span>>> checkOneMap = new ConcurrentHashMap<>(128);
    // 查询2数据结构
    // Map<responder, Map<timestamp, Span>>
    Map<String, Map<Long, Span>> checkTwoMap = new ConcurrentHashMap<>(64);
    double eps = 1e-4;
    public static final DecimalFormat formatter = new DecimalFormat("0.00");
    private static final String[] dataArray = new String[7];

    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
    }

    @Override
    public void prepare(String path) {
        formatter.setMaximumFractionDigits(2);
        formatter.setGroupingSize(0);
        formatter.setRoundingMode(RoundingMode.FLOOR);

        try {
            RandomAccessFile memoryMappedFile = new RandomAccessFile(path, "rw");
            FileChannel channel = memoryMappedFile.getChannel();
            // try to use 16KB buffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 64);
            while (channel.read(byteBuffer) != -1) {
                byteBuffer.flip();
                int remain = byteBuffer.remaining();
                byte[] bts = new byte[remain];
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
        for (int i = 0; i < block.length; i++) {
            byte bt = block[i];
            // 逗号
            if (bt == 44) {
                dataArray[splitterTime] = new String(block, prePos + 1, i - prePos - 1);
                prePos = i;
                splitterTime += 1;
            }
            // 换行符
            if (bt == 10) {
                // 处理完整行
                if (!firstLine) {
                    dataArray[splitterTime] = new String(block, prePos + 1, i - prePos - 1);
                    handleLine(dataArray);
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

    private void handleLine(String[] dataArray) {
        String callerService = dataArray[0];
        String callerIp = dataArray[1];
        String responderService = dataArray[2];
        String responderIp = dataArray[3];
        String isSuccess = dataArray[4];
        short costTime = Short.parseShort(dataArray[5]);
        // 向上取整
        long fullMinute = computeSecond(Long.parseLong(dataArray[6]));
        //Map<(caller, responder), Map<timestamp, Map<(callerIp, responderIp), Object(list[costTime...costTime], sucTime, totalTime)>>>
        Map<Long, Map<String, Span>> timestampMap;
        Map<String, Span> ipPairMap;
        Span span;
        String serviceKey = callerService + responderService;
        String ipPairKey = callerIp + "," + responderIp;
        // 实测computeIfAbsent和putIfAbsent都会比较慢，所以使用原始做法
        if ((timestampMap = checkOneMap.get(serviceKey)) == null) {
            timestampMap = new ConcurrentHashMap<>();
            checkOneMap.put(serviceKey, timestampMap);
        }
        if ((ipPairMap = timestampMap.get(fullMinute)) == null) {
            ipPairMap = new ConcurrentHashMap<>();
            timestampMap.put(fullMinute, ipPairMap);
        }
        if ((span = ipPairMap.get(ipPairKey)) == null) {
            span = new Span();
            ipPairMap.put(ipPairKey, span);
        }
        span.update(costTime, isSuccess);

        // 记录第二种查询数据
        Map<Long, Span> tmpMap;
        Span dataLessSpan;
        if ((tmpMap = checkTwoMap.get(responderService)) == null) {
            tmpMap = new ConcurrentHashMap<>();
            checkTwoMap.put(responderService, tmpMap);
        }
        if ((dataLessSpan = tmpMap.get(fullMinute)) == null) {
            dataLessSpan = new Span();
            tmpMap.put(fullMinute, dataLessSpan);
        }
        dataLessSpan.sucTime += ("true".equals(isSuccess) ? 1 : 0);
        dataLessSpan.totalTime += 1;
    }

    private void handleLine(String line) {
        String[] dataArray = line.split(",");
        handleLine(dataArray);
    }

    private long computeSecond(long timestamp) {
        return (timestamp / 60000) * 60000;
    }
    int tm = 0;
    @Override
    public List<String> checkPair(String caller, String responder, String time) {
        List<String> res = new ArrayList<>();
        String serviceKey = caller + responder;
        Map<Long, Map<String, Span>> timestampMap;
        Map<String, Span> ipPairMap;
        Date date = null;
        try {
            date = dateFormat.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if ((timestampMap = checkOneMap.get(serviceKey)) != null && date != null) {
            if ((ipPairMap = timestampMap.get(date.getTime())) != null) {
                Set<Map.Entry<String, Span>> entries = ipPairMap.entrySet();
                entries.forEach((entry) -> {
                    String ipPair = entry.getKey();
                    Span span = entry.getValue();

                    double sucRate = (double) span.sucTime / span.totalTime;
                    String strSucRate;
                    if (span.sucTime == 0) {
                        strSucRate = ".00%";
                    } else {
                        strSucRate = formatDouble(sucRate * 100) + "%";
                    }
                    int p99 = span.getP99();
                    res.add(ipPair + "," + strSucRate + "," + p99);
                });
            }
        }
        tm++;
        if (tm >= 1) throw new RuntimeException("what???");
        return res;
    }

    @Override
    public String checkResponder(String responder, String start, String end) {
        Map<Long, Span> timestampMap = checkTwoMap.get(responder);
        if (timestampMap == null) return "-1.00%";
        Date startDate = null;
        Date endDate = null;
        try {
            // TODO rewrite parse
            startDate = dateFormat.parse(start);
            endDate = dateFormat.parse(end);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long startMil = startDate.getTime();
        long endMil = endDate.getTime();
        double times = 0;
        double sum = 0;
        Span span;
        for (long i = startMil; i <= endMil; i += 60000) {
            span = timestampMap.get(i);
            if (span != null && span.totalTime != 0) {
                String str = formatDouble((double) span.sucTime / span.totalTime * 100);
                sum += Double.parseDouble(str);
                times++;
            }
        }
        if (sum == 0) {
            if (times == 0) {
                return "-1.00%";
            }
            return ".00%";
        }
        String s = formatDouble(sum / times);
        return s + "%";
    }

    public String formatDouble(double num) {
        String tmp = String.valueOf(num);
        int i = tmp.indexOf(".");
        String res;
        int len = tmp.length();
        if (i + 3 <= len) {
            res = tmp.substring(0, i + 3);
        } else {
            res = tmp;
            for (int j = len; j < i + 3; j++) {
                res += "0";
            }
        }
        return res;
    }

    static class Span {
        int sucTime;
        int totalTime;
        // 桶排,数量要是超过Short.MAX_VALUE就错了！！！
        int[] bucket = new int[200];

        public Span() {
            sucTime = 0;
            totalTime = 0;
        }

        public void update(short costTime, String isSuccess) {
            // bucket不够大就扩容！
            if (costTime >= bucket.length) {
                int[] newBct = new int[costTime + 30];
                System.arraycopy(bucket, 0, newBct, 0, bucket.length);
                bucket = newBct;
            }
            totalTime += 1;
            sucTime += ("true".equals(isSuccess) ? 1 : 0);
            bucket[costTime] += 1;
        }

        public int getP99() {
            int pos = (int) (totalTime * 0.01) + 1;
            int len = bucket.length;
            for (int i = len - 1; i >= 0; i--) {
                pos -= bucket[i];
                if (pos <= 0) return i;
            }
            return 0;
        }
    }
}