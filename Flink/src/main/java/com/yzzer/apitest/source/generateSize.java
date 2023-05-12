package com.yzzer.apitest.source;

public class generateSize {
    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        String str = "afiahfiahfiahsfuihaiusfhaisfhaisfiuahsfai" +
                "asdasdasdasdasd" +
                "asdasdasdadaasfasfgargeqrgeqrg" +
                "eqrgeqrgqergqegqegqergegqgeqrg" +
                "qergqergeqrgqegergeqgeqg";
        long sum = 0L;
        while (System.currentTimeMillis() < time + 1000L){
            for (int i = 0; i < 1000; i++) {
                sum += str.getBytes().length;
            }
        }
        System.out.println(sum);
    }
}
