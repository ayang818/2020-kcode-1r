package com.kuaishou.kcode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author kcode
 * Created on 2020-05-28
 */
public class KcodeUtils {

    public static Map<CheckPairKey, Set<String>> createCheckPairMap(String checkPairFilePath) {
        Map<CheckPairKey, Set<String>> checkMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(checkPairFilePath)))) {
            Set<String> result = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("checkPair|")) {
                    String[] split = line.split("\\|");
                    CheckPairKey checkPairKey = new CheckPairKey(split[1]);
                    result = new HashSet<>();
                    checkMap.put(checkPairKey, result);
                    continue;
                }
                if (!line.equals("null")) {
                    result.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return checkMap;
    }

    public static Map<CheckResponderKey, String> createCheckResponderMap(String checkResponderFilePath) {
        Map<CheckResponderKey, String> checkResponderMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(checkResponderFilePath)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(",");
                CheckResponderKey checkPairKey = new CheckResponderKey(split[0], split[1], split[2]);
                checkResponderMap.put(checkPairKey, split[3]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return checkResponderMap;
    }
}