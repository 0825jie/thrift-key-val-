import org.apache.thrift.TException;

import java.util.List;

/**
 * Created by jason on 18/4/22.
 */
public class unit_tests {

    private static KeyValHandler test;
    private static String s1 = "A1", s2 = "A2", s3 = "A3", s4 = "A4", s5 = "A5";

    public static void main(String[] args) throws TException {
        test = new KeyValHandler();
        putTest();
        getTest();
        getByTimeTest();
        diffTest();
        delKeyValTest();
        getTest();
        delKeyTest();
        getTest();
        getByTimeTest2();
        putTest2();
        getTest();
        getByTimeTest3();

    }

    private static void putTest() throws TException {
        System.out.println("---------putTest 1----------");

        System.out.println("putTest 1: put key-val A1-A2");
        boolean res1 = test.put1(s1, s2);
        System.out.println("Result: " + res1 + "  [True : success add key - val; False : key = val] \n");

        System.out.println("putTest 2: put key-val A1-A3");
        boolean res2 = test.put1(s1, s3);
        System.out.println("Result: " + res2 + "\n");

        System.out.println("putTest 3: put key-val A1-A4");
        boolean res3 = test.put1(s1, s4);
        System.out.println("Result: " + res3 + "\n");

        System.out.println("putTest 4: put key-val A1-A2");
        boolean res4 = test.put1(s1, s2);
        System.out.println("Result: " + res4 + "\n");

        System.out.println("putTest 5: put key-val A3-A4");
        boolean res5 = test.put1(s3, s4);
        System.out.println("Result: " + res5 + "\n");

        System.out.println("putTest 6: put key-val A2-A5");
        boolean res6 = test.put1(s2, s5);
        System.out.println("Result: " + res6 + "\n");

        System.out.println("putTest 7: put key-val A2-A2");
        boolean res7 = test.put1(s2, s2);
        System.out.println("Result: " + res7 + "\n");

    }

    private static void putTest2() throws TException {
        System.out.println("---------putTest2 ----------");
        System.out.println("putTest2 1: put key-val A1-A3");
        boolean res1 = test.put1(s1, s3);
        System.out.println("Result: " + res1 + "  [True = add a new val to key; False = update val to key] \n");

        System.out.println("putTest2 2: put key-val A1-A4");
        boolean res2 = test.put1(s1, s4);
        System.out.println("Result: " + res2 + "\n");

        System.out.println("putTest2 3: put key-val A1-A5");
        boolean res3 = test.put1(s1, s5);
        System.out.println("Result: " + res3 + "\n");

        System.out.println("putTest2 4: put key-val A2-A4");
        boolean res4 = test.put1(s2, s4);
        System.out.println("Result: " + res4 + "\n");

        System.out.println("putTest2 5: put key-val A2-A1");
        boolean res5 = test.put1(s2, s1);
        System.out.println("Result: " + res5 + "\n");

        System.out.println("putTest2 6: put key-val A4-A5");
        boolean res6 = test.put1(s4, s5);
        System.out.println("Result: " + res6 + "\n");

        System.out.println("putTest2 7: put key-val A3-A2");
        boolean res7 = test.put1(s3, s2);
        System.out.println("Result: " + res7 + "\n");
    }


    private static void getTest() throws TException {
        System.out.println("---------getTest ----------");
        System.out.println("getTest2 1: get value of key A1");
        List<String> res1 = test.get1(s1);
        print_get_test(res1);

        System.out.println("getTest2 2: get value of key A2");
        List<String> res2 = test.get1(s2);
        print_get_test(res2);

        System.out.println("getTest2 3:get value of key A3");
        List<String> res3 = test.get1(s3);
        print_get_test(res3);

        System.out.println("getTest2 4:get value of key A4");
        List<String> res4 = test.get1(s4);
        print_get_test(res4);

        System.out.println("getTest2 5:get value of key A5");
        List<String> res5 = test.get1(s5);
        print_get_test(res5);
    }

    private static void getByTimeTest() throws TException {
        System.out.println("---------getByTimeTest ----------");
        System.out.println("getByTimeTest 1: get value of key A1 until 0");
        List<String> res1 = test.get2(s1, (long) 0);
        print_get_test(res1);

        System.out.println("getByTimeTest 2: get value of key A1 until 1");
        List<String> res2 = test.get2(s1, (long) 1);
        print_get_test(res2);

        System.out.println("getByTimeTest 3: get value of key A1 until 2");
        List<String> res3 = test.get2(s1, (long) 2);
        print_get_test(res3);

        System.out.println("getByTimeTest 4: get value of key A1 until 5");
        List<String> res4 = test.get2(s1, (long) 5);
        print_get_test(res4);

        System.out.println("getByTimeTest 5: get value of key A2 until 5");
        List<String> res5 = test.get2(s2, (long) 5);
        print_get_test(res5);

        System.out.println("getByTimeTest 6: get value of key A3 until 10");
        List<String> res6 = test.get2(s3, (long) 10);
        print_get_test(res6);

        System.out.println("getByTimeTest 7: get value of key A4 until 10");
        List<String> res7 = test.get2(s4, (long) 10);
        print_get_test(res7);

        System.out.println("getByTimeTest 8: get value of key A5 until 20");
        List<String> res8 = test.get2(s5, (long) 20);
        print_get_test(res8);

    }

    private static void getByTimeTest2() throws TException {
        System.out.println("---------getByTimeTest2 ----------");
        System.out.println("getByTimeTest2 1: get value of key A1 until 10");
        List<String> res1 = test.get2(s1, (long) 10);
        print_get_test(res1);

        System.out.println("getByTimeTest2 2: get value of key A2 until 10");
        List<String> res2 = test.get2(s2, (long) 10);
        print_get_test(res2);

        System.out.println("getByTimeTest2 3: get value of key A3 until 10");
        List<String> res3 = test.get2(s3, (long) 10);
        print_get_test(res3);

        System.out.println("getByTimeTest2 4: get value of key A3 until 40");
        List<String> res4 = test.get2(s4, (long) 40);
        print_get_test(res4);

        System.out.println("getByTimeTest2 5: get value of key A5 until 50");
        List<String> res5 = test.get2(s5, (long) 50);
        print_get_test(res5);

    }

    private static void getByTimeTest3() throws TException {
        System.out.println("---------getByTimeTest3 ----------");
        System.out.println("getByTimeTest2 1: get value of key A1 until 100");
        List<String> res1 = test.get2(s1, (long) 100);
        print_get_test(res1);

        System.out.println("getByTimeTest2 2: get value of key A2 until 100");
        List<String> res2 = test.get2(s2, (long) 100);
        print_get_test(res2);

        System.out.println("getByTimeTest2 3: get value of key A3 until 100");
        List<String> res3 = test.get2(s3, (long) 100);
        print_get_test(res3);

        System.out.println("getByTimeTest2 4: get value of key A3 until 100");
        List<String> res4 = test.get2(s4, (long) 100);
        print_get_test(res4);

        System.out.println("getByTimeTest2 5: get value of key A5 until 100");
        List<String> res5 = test.get2(s5, (long) 100);
        print_get_test(res5);


    }


    private static void diffTest() throws TException {
        System.out.println("---------diffTest ----------");
        System.out.println("diffTest 0: get value of key A1 between 0 and 5");
        List<String> res0 = test.diff(s1, (long) 0, (long) 5);
        print_get_test(res0);

        System.out.println("diffTest 1: get value of key A1 between 0 and 1");
        List<String> res1 = test.diff(s1, (long) 0, (long) 1);
        print_get_test(res1);

        System.out.println("diffTest 2: get value of key A1 between 1 and 2");
        List<String> res2 = test.diff(s1, (long) 1, (long) 2);
        print_get_test(res2);

        System.out.println("diffTest 3: get value of key A1 between 2 and 3");
        List<String> res3 = test.diff(s1, (long) 2, (long) 3);
        print_get_test(res3);

        System.out.println("diffTest 4: get value of key A1 between 3 and 4");
        List<String> res4 = test.diff(s1, (long) 3, (long) 4);
        print_get_test(res4);

        System.out.println("diffTest 5: get value of key A1 between 4 and 5");
        List<String> res5 = test.diff(s1, (long) 4, (long) 5);
        print_get_test(res5);

        System.out.println("diffTest 6: get value of key A2 between 2 and 5");
        List<String> res6 = test.diff(s2, (long) 2, (long) 5);
        print_get_test(res6);

        System.out.println("diffTest 7: get value of key A5 between 10 and 20");
        List<String> res7 = test.diff(s5, (long) 10, (long) 20);
        print_get_test(res7);

    }

    private static void delKeyValTest() throws TException {
        System.out.println("---------delKeyValTest ----------");
        System.out.println("delKeyValTest 1: delete key-val A1-A2");
        boolean res1 = test.del2(s1, s2);
        print_delet_info(res1);

        System.out.println("delKeyValTest 2: delete key-val A1-A2");
        boolean res2 = test.del2(s1, s2);
        print_delet_info(res2);

        System.out.println("delKeyValTest 3: delete key-val A2-A1");
        boolean res3 = test.del2(s2, s1);
        print_delet_info(res3);

        System.out.println("delKeyValTest 4: delete key-val A1-A3");
        boolean res4 = test.del2(s1, s3);
        print_delet_info(res4);
    }

    private static void delKeyTest() throws TException {
        System.out.println("---------delKeyTest ----------");
        System.out.println("delKeyTest 1: delete key A3");
        print_delet_info(test.del1(s3));

        System.out.println("delKeyTest 2: delete key A3");
        print_delet_info(test.del1(s3));

        System.out.println("delKeyTest 3: delete key A4");
        print_delet_info(test.del1(s4));

        System.out.println("delKeyTest 4: delete key A1");
        print_delet_info(test.del1(s1));

        System.out.println("delKeyTest 5: delete key A1");
        print_delet_info(test.del1(s1));

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

    private static void print_deletkeyval_info(int code) {
        if (code == 0) {
            System.out.println("Result: Success!\n");
        } else if (code == 1) {
            System.out.println("Result: Failed! The key does not contain this value!\n");
        } else {
            System.out.println("Result: Failed! There is no such key!\n");
        }
    }

    private static void print_delet_info(boolean res) {
        if (res) {
            System.out.println("Result: Delete key Successfully!\n");
        } else {
            System.out.println("Result: Failed! There is no such key!\n");
        }
    }

}
