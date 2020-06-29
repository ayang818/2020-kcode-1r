package com.kuaishou.kcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// TODO 寻找可以优化的查询结构
// TODO 由于二三阶段都是速度极为敏感的，所以需要尽量把耗时操作（计算），移动到一阶段。

/**
 * @author kcode
 * Created on 2020-06-01
 * 实际提交时请维持包名和类名不变
 */
public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {
    // 行数
    private static final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(8, 8, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    StringBuilder lineBuilder = new StringBuilder();
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    // 数据的所有特点 servicePair极少；timestamp极少，代表每分钟；ipPair也很少，集中在30左右；多的就是调用次数
    // 查询1数据结构
    // Map<hash(caller, responder), Map<timestamp, Map<(callerIp, responderIp), Span>>>
    Map<Integer, Map<Long, Map<String, Span>>> checkOneMap = new ConcurrentHashMap<>(128);
    // 查询2数据结构
    // Map<responder, Map<timestamp, Span>>
    // version2: Map<responder, Span[]> pos = [(timestamp - startTime) / 60000] Span[].length <> 45000
    Map<String, Span[]> checkTwoMap = new ConcurrentHashMap<>(64);
    private static final int spanCapacity = 500000;
    double eps = 1e-4;
    private static final String[] dataArray = new String[7];
    private static long startTime = 0;
    private static final long[] runMonthMillisCount = new long[13];
    private static final long[] runYearMonthDayCount = new long[]{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    private static final long[] normalYearMonthDayCount = new long[]{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    static {
        Date startDate = null;
        try {
            // 一天 86400000
            // 1577808000000
            startDate = dateFormat.parse("2020-01-01 00:00");
            startTime = startDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        runMonthMillisCount[1] = 0;
        for (int i = 2; i <= 12; i++) {
            runMonthMillisCount[i] = runMonthMillisCount[i - 1] + 86400000 * runYearMonthDayCount[i - 1];
        }
    }


    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
    }

    @Override
    public void prepare(String path) {
        try {
            // 64KB，打满吞吐量
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path), 64 * 1024);
            String line;
            // 此数值越小，任务越多，执行时间越短，但是在未作同步的情况下越容易发生并发错误
            int threshold = 5000;
            List<String> list = new ArrayList<>(threshold);
            while ((line = bufferedReader.readLine()) != null) {
                list.add(line);
                if (list.size() >= threshold) {
                    final List<String> tmp = list;
                    threadPool.execute(() -> handleLines(tmp));
                    list = new ArrayList<>(threshold);
                }
            }
            if (list.size() > 0) {
                final List<String> tmp = list;
                threadPool.execute(() -> handleLines(tmp));
            }
            while (threadPool.getQueue().size() != 0) {
            }
            // RandomAccessFile memoryMappedFile = new RandomAccessFile(path, "r");
            // FileChannel channel = memoryMappedFile.getChannel();
            // // try to use 16KB buffer
            // ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 64);
            // int size = 0;
            // while (channel.read(byteBuffer) != -1) {
            //     byteBuffer.flip();
            //     int remain = byteBuffer.remaining();
            //     byte[] bts = new byte[remain];
            //     byteBuffer.get(bts, 0, remain);
            //     byteBuffer.clear();
            //     processBlock(bts);
            //     size += 64;
            // }
        } catch (IOException e) {
            e.printStackTrace();
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
        Integer serviceKey = hash(callerService, responderService);
        String ipPairKey = new StringBuilder().append(callerIp).append(",").append(responderIp).toString();
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
            span = new Span(ipPairKey);
            ipPairMap.put(ipPairKey, span);
        }
        span.update(costTime, isSuccess);

        // 记录第二种查询数据
        Span[] spans;
        Span dataLessSpan;
        if ((spans = checkTwoMap.get(responderService)) == null) {
            spans = new Span[spanCapacity];
            checkTwoMap.put(responderService, spans);
        }
        int hash = minuteHash(fullMinute);
        if ((dataLessSpan = spans[hash]) == null) {
            dataLessSpan = new Span();
            spans[hash] = dataLessSpan;
        }
        int tmpSucTime = dataLessSpan.sucTime.addAndGet("true".equals(isSuccess) ? 1 : 0);
        int tmpTotalTime = dataLessSpan.totalTime.addAndGet(1);
        dataLessSpan.sucRate = ((double) tmpSucTime / tmpTotalTime);
    }

    private void handleLine(String line) {
        String[] dataArray = line.split(",");
        handleLine(dataArray);
    }

    private void handleLines(List<String> tmp) {
        int len = tmp.size();
        for (int i = 0; i < len; i++) {
            handleLine(tmp.get(i));
        }
    }

    private long computeSecond(long timestamp) {
        return (timestamp / 60000) * 60000;
    }

    private Integer hash(String a, String b) {
        int result = 1;
        result = 31 * result + a.hashCode();
        result = 31 * result + b.hashCode();
        return result;
    }

    public int minuteHash(long timezone) {
        return (int) ((timezone - startTime) / 60000L);
    }

    @Override
    public List<String> checkPair(String caller, String responder, String time) {
        List<String> res = new ArrayList<>(20);
        Integer serviceKey = hash(caller, responder);
        Map<Long, Map<String, Span>> timestampMap;
        // key: ipPair, value: Span
        Map<String, Span> ipPairMap;
        long timeMillis = parseDate(time);
        if ((timestampMap = checkOneMap.get(serviceKey)) != null) {
            // 拿到对应服务在此时间戳下的所有记录
            if ((ipPairMap = timestampMap.get(timeMillis)) != null) {
                Iterator<Map.Entry<String, Span>> iterator = ipPairMap.entrySet().iterator();
                Map.Entry<String, Span> entry;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    res.add(entry.getValue().getRes());
                }
            }
        }
        return res;
    }

    @Override
    public String checkResponder(String responder, String start, String end) {
        Span[] timestampMap = checkTwoMap.get(responder);
        if (timestampMap == null) return "-1.00%";
        long startMil = parseDate(start);
        long endMil = parseDate(end);
        double times = 0;
        double sum = 0;
        Span span;
        for (long i = startMil; i <= endMil; i += 60000) {
            span = timestampMap[minuteHash(i)];
            if (span != null && span.getSucTime() != 0) {
                sum += (span.getSucRate() * 100);
                times++;
            }
        }
        if (sum == 0) {
            if (times == 0) {
                return "-1.00%";
            }
            return ".00%";
        }
        return formatDouble(sum / times) + "%";
    }

    public static String formatDouble(double num) {
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


    /**
     * TODO 当前只对于2020的日志有作用，晚点改?
     *
     * @param dateStr dateString format 2020-06-01 09:42
     * @return
     */
    public static long parseDate(String dateStr) {
        long res = startTime;
        int mouth = Integer.parseInt(dateStr.substring(5, 7));
        int day = Integer.parseInt(dateStr.substring(8, 10));
        int hour = Integer.parseInt(dateStr.substring(11, 13));
        int minutes = Integer.parseInt(dateStr.substring(14, 16));
        res += runMonthMillisCount[mouth];
        res += (day - 1) * 86400000;
        res += hour * 3600000;
        res += minutes * 60000;
        return res;
    }

    static class Span {
        AtomicInteger sucTime;
        AtomicInteger totalTime;
        double sucRate;

        // 桶排,数量要是超过Short.MAX_VALUE就错了！！！
        int[] bucket;
        String ipPair;
        String res;

        public Span() {
            sucTime = new AtomicInteger(0);
            totalTime = new AtomicInteger(0);
        }

        public Span(String ipPair) {
            this.ipPair = ipPair;
            sucTime = new AtomicInteger(0);
            totalTime = new AtomicInteger(0);
            bucket = new int[200];
        }

        public synchronized void update(short costTime, String isSuccess) {
            // bucket不够大就扩容！
            if (costTime >= bucket.length) {
                int[] newBct = new int[costTime + 30];
                System.arraycopy(bucket, 0, newBct, 0, bucket.length);
                bucket = newBct;
            }
            int tmpTotalTime = totalTime.addAndGet(1);
            int tmpSucTime = sucTime.addAndGet("true".equals(isSuccess) ? 1 : 0);
            sucRate = ((double) tmpSucTime / tmpTotalTime);
            bucket[costTime] += 1;

            // generate res
            double sucRate = getSucRate();
            String strSucRate;
            if (getSucTime() == 0) {
                strSucRate = ".00";
            } else {
                strSucRate = formatDouble(sucRate * 100);
            }
            int p99 = getP99();
            this.res = new StringBuilder().append(ipPair).append(",").append(strSucRate).append("%,").append(p99).toString();
        }

        public int getP99() {
            int pos = (int) (totalTime.get() * 0.01) + 1;
            int len = bucket.length;
            for (int i = len - 1; i >= 0; i--) {
                pos -= bucket[i];
                if (pos <= 0) return i;
            }
            return 0;
        }

        public int getTotalTime() {
            return totalTime.get();
        }

        public int getSucTime() {
            return sucTime.get();
        }

        public double getSucRate() {
            return sucRate;
        }

        public String getRes() {
            return this.res;
        }
    }


    @Deprecated
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
}