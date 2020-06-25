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
import java.util.concurrent.PriorityBlockingQueue;

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
    double inf = 1e-8;
    public static int m;
    public static DecimalFormat formatter = new DecimalFormat("#.00");
    static {
        formatter.setMaximumFractionDigits(4);
        formatter.setGroupingSize(0);
        formatter.setRoundingMode(RoundingMode.FLOOR);
    }
    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
    }

    @Override
    public void prepare(String path) {
        try {
            RandomAccessFile memoryMappedFile = new RandomAccessFile(path, "rw");
            FileChannel channel = memoryMappedFile.getChannel();
            // try to use 16KB buffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 64);
            long size = 0;
            while (channel.read(byteBuffer) != -1) {
                byteBuffer.flip();
                int remain = byteBuffer.remaining();
                byte[] bts = new byte[remain];
                byteBuffer.get(bts, 0, remain);
                byteBuffer.clear();
                processBlock(bts);
                size += remain;
                // System.out.println(String.format("%dMB", size / 1024 / 1024));
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
        String ipKey = callerIp + "," + responderIp;
        // 实测computeIfAbsent和putIfAbsent都会比较慢，所以使用原始做法
        if ((timestampMap = checkOneMap.get(serviceKey)) == null) {
            timestampMap = new ConcurrentHashMap<>();
            checkOneMap.put(serviceKey, timestampMap);
        }
        if ((ipPairMap = timestampMap.get(fullMinute)) == null) {
            ipPairMap = new ConcurrentHashMap<>();
            timestampMap.put(fullMinute, ipPairMap);
        }
        if ((span = ipPairMap.get(ipKey)) == null) {
            span = new Span();
            ipPairMap.put(ipKey, span);
        }
        // System.out.println(String.format("checkOneMap size %d, timestampMap size %d, ipPairSize %d, ipPair len %d}", checkOneMap.size(), timestampMap.size(), ipPairMap.size()));
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
        return timestamp / 60000 * 60000;
    }

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
                    if (sucRate - 0.00 < inf) {
                        strSucRate = ".00%";
                    } else {
                        strSucRate = formatter.format(sucRate * 100) + "%";
                    }
                    int p99 = span.getP99();
                    res.add(ipPair + "," + strSucRate + "," + p99);
                });
            }
        }
        return res;
    }

    @Override
    public String checkResponder(String responder, String start, String end) {
        Map<Long, Span> timestampMap = checkTwoMap.get(responder);
        if (timestampMap == null) return "-1.00%";
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = dateFormat.parse(start);
            endDate = dateFormat.parse(end);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long startMil = startDate.getTime();
        long endMil = endDate.getTime();
        int times = 0;
        double sum = 0;
        Span span;
        for (long i = startMil; i <= endMil; i += 60000) {
            span = timestampMap.get(i);
            if (span != null && span.totalTime != 0) {
                sum += ((double) span.sucTime / span.totalTime);
                times++;
            }
        }
        if (sum == 0) return "-1.00%";
        return formatter.format(sum / times * 100) + "%";
    }

    class Span {
        // 小顶堆
        PriorityBlockingQueue<Short> heap;
        int sucTime;
        int totalTime;
        int maxHeapSize = 2000;

        public Span() {
            sucTime = 0;
            totalTime = 0;
        }

        public void update(short costTime, String isSuccess) {
            if (heap == null) heap = new PriorityBlockingQueue<>(1000);
            totalTime += 1;
            sucTime += ("true".equals(isSuccess) ? 1 : 0);
            if (heap.size() >= maxHeapSize && costTime > heap.peek()) {
                heap.poll();
                heap.offer(costTime);
            }
            if (heap.size() < maxHeapSize) {
                heap.offer(costTime);
            }
        }

        public int getP99() {
            int res;
            int targetSize = (int) (totalTime * 0.01) + 1;
            while (heap.size() > targetSize) {
                heap.poll();
            }
            res = heap.peek();
            return res;
        }
    }
}



/*
查询1（checkPair）：
输入：主调服务名、被调服务名和时间（分钟粒度）
输出：返回在这一分钟内主被调按ip聚合的成功率和P99, 无调用返回空list （不要求顺序）
输入示例：commentService，userService，2020-06-01 09:42
输出示例：
172.17.60.3,172.17.60.4,50.00%,79
172.17.60.2,172.17.60.3,100.00%,103

1. 查询1对应数据结构
(caller,responder) -> timestamp -> (callerIp, responderIp) -> (heap[costTime, costTime], sucTime, totalTime)
Map<(caller, responder), Map<timestamp, Map<(callerIp, responderIp), Object(heap[costTime...costTime], sucTime, totalTime)>>>

imService31,10.190.246.247,openService4,10.163.127.219,true,202,1592301779942
imService31,10.101.62.175,openService4,10.92.133.81,true,115,1592301779419
String callerService = dataArray[0];
String callerIp = dataArray[1];
String responderService = dataArray[2];
String responderIp = dataArray[3];
String isSuccess = dataArray[4];
String costTime = dataArray[5];
// 向上取整
long fullSecond = computeSecond(Long.parseLong(dataArray[6]));

2. 查询2数据结构
responder -> timestamp -> Object(heap[costTime...costTime], sucTime, totalTime)
Map<responder, Map<timestamp, Span>>

查询2（checkResponder）：
输入： 被调服务名、开始时间和结束时间
输出：平均成功率，无调用结果返回 -1.00%
平均成功率 = （被调服务在区间内各分钟成功率总和）/ (存在调用的分钟数）【结果换算成百分比，小数点后保留两位数字,不足两位补0，其他位直接舍弃，不进位】
输入示例：userService, 2020-06-01 09:42, 2020-06-01 09:44
输出示例：66.66%
计算过程示例：
（2020-06-01 09:42）userService被调成功率: 4/6= 66.66%
（2020-06-01 09:43）userService无调用
（2020-06-01 09:44）userService无调用
 那么userService在2020-06-01 09:42到2020-06-01 09:44的平均成功率为（66.66%）/ 1 = 66.66%【示例数据仅仅在2020-06-01 09:42有调用】
 */

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

