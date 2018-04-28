import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Created by jason on 18/4/22.
 */
public class KeyValClient {

    public static TTransport transport;
    private static boolean stop;
    public static void main(String [] args) {

        try {
            transport= new TSocket("localhost", 9090);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            MultiplicationService.Client client = new MultiplicationService.Client(protocol);

            perforPrompt();
            readStr(client);

        } catch (TException x) {
            x.printStackTrace();
        }
    }

    private static void readStr(MultiplicationService.Client client) throws TException {
        stop = false;
        Scanner scan ;
        String string ;
       while(!stop){
           System.out.print("Enter command or ['help']:");
           scan = new Scanner(System.in);
            string = scan.nextLine();
           process(string, client);

       }
    }

    private static void process(String str, MultiplicationService.Client client) throws TException {
        String[] strs = str.split(" +");
        if(strs.length == 0 || strs.length > 4) return;
        int len = strs.length;
        if(len == 1){
            if(strs[0].equals("time")){
                checkTime(client);
            }else if(strs[0].equals("help") ){
                perforPrompt();

            }else if(strs[0].equals("exit")){
                doClose();
                return;
            }else{
                erroInfo();
            }

        }else if(len == 2){
            if(strs[0].equals("get") ){
                getValue(client, strs[1]);
            }else if(strs[0].equals("delete") ){
                deleteKey(client, strs[1]);
            }else{
                erroInfo();
            }

        }else if(len == 3){
            if(strs[0].equals("get") ){
                if(isInteger(strs[2])){
                    getValByTime(client, strs[1], strs[2]);
                    return;
                }

            }else if(strs[0].equals("delete")){
                deleteKeyVal(client, strs[1], strs[2]);
                return;
            }else if(strs[0].equals("put")){
                putKeyVal(client, strs[1], strs[2]);
                return;
            }
                erroInfo();

        }else{
            if(strs[0].equals("diff")){
                if(isInteger(strs[2]) && isInteger(strs[3])){
                    getPartValue(client, strs[1], strs[2], strs[3]);
                    return;
                }

            }
                erroInfo();

        }
    }

    public static boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }

    private static void doClose() {
        System.out.println("-----Client End-----");
        stop = true;
        transport.close();

    }

    private static void checkTime(MultiplicationService.Client client) throws TException {
        Long res = client.getTime();
        System.out.println("Now Time is: "+res);

    }
    private static void erroInfo() throws TException {

        System.out.println("Invalid input, please check 'help' and re-enter");

    }
    private static void getValue(MultiplicationService.Client client, String key) throws TException {
        List<String> res = client.get1(key);
        print_get_test(res);
    }
    private static void getValByTime(MultiplicationService.Client client, String key, String t1) throws TException {
        Long time = Long.valueOf(t1);
        List<String> res = client.diff(key,(long)-1, time);
        print_get_test(res);
    }

    private static void getPartValue(MultiplicationService.Client client, String key, String t1, String t2) throws TException {
        Long time1 = Long.valueOf(t1);
        Long time2 = Long.valueOf(t2);
        List<String> res = client.diff(key, time1, time2);
        print_get_test(res);
    }

    private static void deleteKey(MultiplicationService.Client client, String key) throws TException {
        boolean res = client.del1(key);
        print_delet_info(res);
    }
    private static void deleteKeyVal(MultiplicationService.Client client, String key, String val) throws TException {
        boolean res = client.del2(key, val);
        print_delet_info(res);
    }
    private static void putKeyVal(MultiplicationService.Client client, String key, String val) throws TException {
        boolean res = client.put1(key, val);
        System.out.println("Result: "+res);
    }

    private static void perforPrompt() throws TException {
        System.out.println("\nplease follow the format:");
        System.out.println("Example: key: Jason, value = Alice");
        System.out.println("[enter: exit]--------End program");
        System.out.println("[enter: time]--------Get current time,");
        System.out.println("[enter: put Jason Alice]---put key and value ");
        System.out.println("[enter: get Jason]--------Get value(s) of key ");
        System.out.println("[enter: get Jason 20]-----Get value(s) of key in a given time ");
        System.out.println("[enter: delete Jason Alice]--Delete specific value of the key ");
        System.out.println("[enter: delete Jason]--------Delete the key, ");
        System.out.println("[enter: diff Jason 20 30 ]-----Get the value(s) from time1(20) to time2(30), ");
        return;
    }

    private static void print_get_test(List<String> list) {
        System.out.println("Result is:");
        if (list == null || list.size() == 0) {
            System.out.println("[ - NULL- ]\n");
            return;
        }
        System.out.print("[ ");
        for (String str : list) {
            System.out.print(str + " ");
        }
        System.out.println("]\n");
        return;
    }

    private static void print_delet_info(boolean res) {
        if (res) {
            System.out.println("Result: Delete key Successfully!\n");
        } else {
            System.out.println("Result: Failed! There is no such key(-val pair) !\n");
        }
    }

}
