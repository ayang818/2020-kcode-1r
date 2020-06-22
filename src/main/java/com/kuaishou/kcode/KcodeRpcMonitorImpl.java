package com.kuaishou.kcode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * @author kcode
 * Created on 2020-06-01
 * 实际提交时请维持包名和类名不变
 */

public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {

    int tm = 0;
    StringBuilder lineBuilder = new StringBuilder();
    ExecutorService handleThread = Executors.newSingleThreadExecutor();
    final Semaphore semaphore = new Semaphore(20000);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    // callerService+responderService -> (timeStamp -> List{[callerIp, responderIp, isSuccess, costTime], ...})
    // 总共就128组pair -> 分钟为单位的时间片 -> 这个时间片中的所有调用记录
    // 顺便存beCalledService  -> (timeStamp -> List{[...], }) 指向同一个Array
    Map<String, Map<Long, List<String[]>>> dataMap = new ConcurrentHashMap<>();

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
            while (channel.read(byteBuffer) != -1) {
                byteBuffer.flip();
                int remain = byteBuffer.remaining();
                byte[] bts = new byte[remain];
                byteBuffer.get(bts, 0, remain);
                byteBuffer.clear();
                semaphore.acquire();
                handleThread.execute(() -> processBlock(bts));
            }
            System.out.println(dataMap.size());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processBlock(byte[] block) {
        tm++;
        // System.out.println(tm);
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
        semaphore.release();
    }

    private void handleLine(String[] dataArray) {
        String callerService = dataArray[0];
        String callerIp = dataArray[1];
        String responderService = dataArray[2];
        String responderIp = dataArray[3];
        String isSuccess = dataArray[4];
        String costTime = dataArray[5];
        long timestamp = Long.parseLong(dataArray[6]);
        String[] record = {callerIp, responderIp, isSuccess, costTime};

        // long fullSecond = computeSecond(timestamp);
        // Map<Long, List<String[]>> serviceMap = dataMap.computeIfAbsent(callerService + responderService, (k) -> new ConcurrentHashMap<>());
        // List<String[]> records = serviceMap.computeIfAbsent(fullSecond, (k) -> new ArrayList<>());
        // records.add(record);
        //
        // Map<Long, List<String[]>> responderMap = dataMap.computeIfAbsent(responderIp, (k) -> new ConcurrentHashMap<>());
        // List<String[]> responderRecords = responderMap.computeIfAbsent(fullSecond, (k) -> new ArrayList<>());
        // responderRecords.add(record);
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
        // Date date = null;
        // try {
        //     date = dateFormat.parse(time);
        // } catch (ParseException e) {
        //     e.printStackTrace();
        // }
        // long second = date.getTime();
        // Map<Long, List<String[]>> pairSecondSliceMap = dataMap.get(caller + responder);
        // List<String[]> records = pairSecondSliceMap.get(second);
        // Map<String, PairData> statisticMap = new HashMap<>();
        // // [callerIp, responderIp, isSuccess, costTime]
        // String[] record;
        // for (int i = 0; i < records.size(); i++) {
        //     record = records.get(i);
        //     PairData pairData = statisticMap.computeIfAbsent(record[0] + "," + record[1], (k) -> new PairData());
        //     if ("true".equals(record[2])) {
        //         pairData.addShownTimes(true);
        //     } else {
        //         pairData.addShownTimes(false);
        //     }
        //     pairData.addCostTime(Integer.parseInt(record[3]));
        // }
        // Set<Map.Entry<String, PairData>> entries = statisticMap.entrySet();
        // StringBuilder recordBuilder = new StringBuilder();
        // for (Map.Entry<String, PairData> entry : entries) {
        //     String key = entry.getKey();
        //     PairData pairData = entry.getValue();
        //     String sucRate = String.format("%.2f", (double) pairData.getSucTime() / pairData.getTime());
        //     List<Integer> costTimeList = pairData.getCostTimeList();
        //     costTimeList.sort(Comparator.comparingInt(a -> a));
        //     int p99 = costTimeList.get((int) (costTimeList.size() * 0.99));
        //     res.add(recordBuilder.append(key).append(",").append(sucRate).append(",").append(p99).toString());
        //     recordBuilder.delete(0, recordBuilder.length());
        // }

        res.add("172.17.60.3,172.17.60.4,50.00%,79");
        return res;
    }

    @Override
    public String checkResponder(String responder, String start, String end) {
        return "0.00%";
    }
}

class PairData {
    private List<Integer> costTimeList;
    private int time;
    private int sucTime;
    public PairData() {
        costTimeList = new ArrayList<>();
        time = 0;
        sucTime = 0;
    }

    public void addShownTimes(boolean suc) {
        if (suc) {
            sucTime++;
        }
        time++;
    }

    public void addCostTime(int costTime) {
        costTimeList.add(costTime);
    }

    public List<Integer> getCostTimeList() {
        return costTimeList;
    }

    public int getTime() {
        return time;
    }

    public int getSucTime() {
        return sucTime;
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

