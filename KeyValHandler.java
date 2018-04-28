import org.apache.thrift.TException;

import java.util.*;

/**
 * Created by jason on 18/4/23.
 */

public class KeyValHandler implements MultiplicationService.Iface {

    /**
     * assume the data type of the key/value is Long number(as user id);
     *
     * @idMap stores the name(String) - id(Long);
     * @nameMap stores the id(Long) - name(String);
     * @curUsers stores the key-value: blogger and his/her followers, assume the user name is unique
     * (or we can translate into unique one);
     * @history stores the key and followers based on timeline;
     * @index is the number of the user,used as set userID
     * @time is an abstract standard of time, used as timeline
     */

    private Map<String, ArrayList<Long>> idMap;
    private Map<Long, String> namesMap;
    private Map<Long, HashSet<Long>> curUsers;
    private Map<Long, TreeMap<Long, Long>> history;
    private Long index;
    private Long time;

    public KeyValHandler() {
        curUsers = new Hashtable<>();
        history = new Hashtable<>();
        idMap = new Hashtable<>();
        namesMap = new Hashtable<>();
        index = (long) 0;
        time = (long) -1;
    }

    @Override
    public boolean put1(String key, String val) throws TException {

        Long keyId;
        Long valId;
        time++;
        System.out.println("time is:"+ time +", put "+key+", value "+ val);
        /**
         * get the id of key(value) or create id for that key/(val)
         */
        if (idMap.containsKey(val)) {
            valId = getCurIdByKey(val);
        } else {
            valId = insert(val);
        }

        if (idMap.containsKey(key)) {
            keyId = getCurIdByKey(key);
        } else {
            keyId = insert(key);
        }
        /**
         * if the key has been deleted, need to insert into the current users again
         * and update new ID
         */
        if (!curUsers.containsKey(keyId)) {
            keyId = insert(key);
        }
        /**
         * return true(means add value to key) when success;
         * otherwise return false when key = val
         */
        if (keyId == valId) return false;
        insertToHistory(keyId, valId);
        curUsers.get(keyId).add(valId);
        return true;

    }

    @Override
    public List<String> get1(String key) throws TException {
        time++;
        System.out.println("time is:"+ time +", get values of "+key);
        List<String> res = new ArrayList<>();
        if (!idMap.containsKey(key)) {
            return res;
        }
        Long keyId = getCurIdByKey(key);
        if (curUsers.containsKey(keyId)) {
            for (Long id : curUsers.get(keyId)) {
                res.add(namesMap.get(id));
            }
        }
        return res;
    }

    /**
     * get values within t1 equals to get difference from 0 to t1
     * only return the result of last keyid related key
     * $but cannot compile
     */

    @Override
    public List<String> get2(String key, long time1) throws TException {

        List<String> res = diff(key, Long.valueOf(-1), time1);
        return res;
    }



    @Override
    public List<String> get2(String key, Long time1) throws TException {
        time++;
        if (!idMap.containsKey(key)) {
            return null;
        }
        Long keyId = getCurIdByKey(key);

        List<String> res = new ArrayList<>();

        TreeMap<Long, Long> curmap = history.get(keyId);
        if (curmap == null || curmap.size() == 0) return res;
        NavigableMap<Long, Long> subMap = curmap.subMap((long)-1, false, time1, true);
        if (subMap == null || subMap.size() == 0) return res;
        for (Long val : subMap.values()) {
            res.add(namesMap.get(val));
        }
        return res;
    }

    /**
     * use treemap to get the submap of given time scope in log(n)
     * get the last id's of the key(a certain string, maybe the same string put as a key and be deleted
     * then put as a key again,they are different id) with timeline;
     * get the subset of the timeline according to the given time (time1, time2];
     * translate id to string and add to result
     */

    @Override
    public List<String> diff(String key, long time1, long time2) throws TException {
        time++;
        System.out.println("time is:"+ time +", get partial value of "+key);
        if (!idMap.containsKey(key)) {
            return null;
        }
        Long keyId = getCurIdByKey(key);
        if (!history.containsKey(keyId) || time1 >= time2) {
            return null;
        }
        List<String> res = new ArrayList<>();

        TreeMap<Long, Long> curmap = history.get(keyId);
        if (curmap == null || curmap.size() == 0) return res;
        NavigableMap<Long, Long> subMap = curmap.subMap(time1-1, false, time2, true);
        if (subMap == null || subMap.size() == 0) return res;
        for (Long val : subMap.values()) {
            res.add(namesMap.get(val));
        }
        return res;
    }


//    @Override
//    public List<String> diff(String key, Long time1, Long time2) throws TException {
//
//    }

    @Override
    public boolean del1(String key) throws TException {
        time++;
        System.out.println("time is:"+ time +", delete "+key);
        if (!idMap.containsKey(key)) return false;
        Long keyId = getCurIdByKey(key);
        if (curUsers.remove(keyId) == null) {
            return false;
        }
        return true;
    }

    @Override
    public boolean del2(String key, String val) throws TException {

        time++;
        System.out.println("time is:"+ time +", delete "+key+" "+ val);
        if (!idMap.containsKey(key) || !idMap.containsKey(val)) return false;
        Long keyId = getCurIdByKey(key);
        Long valId = getCurIdByKey(val);
        if (!curUsers.containsKey(keyId)) return false;
        if (!curUsers.get(keyId).remove(valId)) return false;
        return true;
    }

    @Override
    public long getTime() {
        time++;
        System.out.println("time is:"+ time +", getTime ");
        return time;
    }

    private Long insert(String str) {
        Long id = ++index;
        if (!idMap.containsKey(str)) {
            List<Long> tp = new ArrayList<>();
            idMap.put(str, (ArrayList<Long>) tp);

        }
        idMap.get(str).add(id);
        namesMap.put(id, str);
        Set<Long> st = new HashSet<>();
        curUsers.put(id, (HashSet<Long>) st);
        return id;
    }

    private void insertToHistory(Long kid, Long vid) {

        if (!history.containsKey(kid)) {
            TreeMap<Long, Long> tm = new TreeMap<>();
            history.put(kid, tm);
        }
        Long curTime = time;
        history.get(kid).put(curTime, vid);

    }

    private Long getCurIdByKey(String k) {
        List<Long> list = idMap.get(k);
        return list.get(list.size() - 1);

    }

}

