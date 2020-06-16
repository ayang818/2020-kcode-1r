package com.kuaishou.kcode;

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

    public void prepare(String path) {
    }

    public List<String> checkPair(String caller, String responder, String time) {
        return new ArrayList<String>();
    }

    public String checkResponder(String responder, String start, String end) {
        return "0.00%";
    }

}
