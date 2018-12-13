package com.panda.hbase.tools;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class GenRowkeyUtil {

    public static String genKey(String id) {
        MessageDigest md;
        try {
            if (id == null || id.isEmpty()) {
                throw new RuntimeException("id is not null or empty at GenRowkeyUtil.genKey()");
            }
            md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(id.getBytes());
            byte[] digest = md.digest();
            StringBuffer sb = new StringBuffer();
            for (byte b : digest) {
                sb.append(Integer.toHexString((int) (b & 0xff)));
            }
            String result = sb.toString();
            return result.substring(1, 2) + result.substring(3, 4) + result.substring(5, 6);

        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("failed init MD5 instance.", ex);
        }
    }

}