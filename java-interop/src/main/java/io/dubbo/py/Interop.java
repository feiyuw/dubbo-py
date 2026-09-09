package io.dubbo.py;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * dubbo-py 与 Java 的 Hessian2 互通测试对端。
 *
 * <pre>
 *   encode            — 输出一组 golden 向量，每行 "名称\u0007hex"
 *   decode &lt;hex&gt;     — 反序列化 hex 并按规范形式打印，供断言
 * </pre>
 */
public class Interop {

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "encode";
        if ("encode".equals(mode)) {
            encodeGolden();
        } else if ("decode".equals(mode)) {
            String hex = args[1];
            Object o = decode(hexToBytes(hex));
            System.out.println(render(o));
        } else {
            System.err.println("unknown mode: " + mode);
            System.exit(2);
        }
    }

    // ------------------------------------------------------------------
    // encode：golden 向量（Java 序列化 → 供 Python 解码断言）
    // ------------------------------------------------------------------

    private static void encodeGolden() throws Exception {
        Map<String, Object> fixtures = new LinkedHashMap<>();

        // 整数 / 长整型
        fixtures.put("i0", 0);
        fixtures.put("i1", 1);
        fixtures.put("i_neg1", -1);
        fixtures.put("i47", 47);
        fixtures.put("i_neg16", -16);
        fixtures.put("i1000", 1000);
        fixtures.put("i_neg1000", -1000);
        fixtures.put("i190000", 190000);
        fixtures.put("i_neg190000", -190000);
        fixtures.put("i300000", 300000);
        fixtures.put("i_max", 2147483647);
        fixtures.put("i_min", -2147483648);
        // long 类型（超出 int32，走 long 编码）
        fixtures.put("long_2_40", 1099511627776L);      // 2^40
        fixtures.put("long_3billion", 3000000000L);
        fixtures.put("long_neg_3billion", -3000000000L);
        fixtures.put("long_1234567890", 1234567890L);

        // 浮点
        fixtures.put("d0", 0.0d);
        fixtures.put("d1", 1.0d);
        fixtures.put("d127", 127.0d);
        fixtures.put("d_neg127", -127.0d);
        fixtures.put("d1_123", 1.123d);
        fixtures.put("d0_12345", 0.12345d);

        // 布尔 / null
        fixtures.put("b_true", true);
        fixtures.put("b_false", false);
        fixtures.put("n_null", null);

        // 字符串（短 / 中 / 长 / 中文 / 非 BMP）
        fixtures.put("s_short", "abcde");
        fixtures.put("s_empty", "");
        fixtures.put("s_100", repeat('a', 100));
        fixtures.put("s_10000", repeat('a', 10000));
        fixtures.put("s_70000", repeat('a', 70000));
        fixtures.put("s_zh", repeat("长字符串", 20000)); // 80000 个 UTF-16 单元
        fixtures.put("s_emoji", "😀");

        // 日期（UTC 毫秒）
        fixtures.put("date_utc", new Date(1532961664062L)); // 2018-07-30T14:41:04.062Z

        // 二进制
        fixtures.put("bytes_3", new byte[]{1, 2, 3});
        fixtures.put("bytes_300", repeatBytes(300, (byte) 0x2a));
        fixtures.put("bytes_70000", repeatBytes(70000, (byte) 0x2a)); // 触发多块 'b'/'B' 分块

        // 容器
        List<Integer> l2 = new ArrayList<>(Arrays.asList(0, 1));
        fixtures.put("list_2", l2);
        List<Integer> l8 = new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7));
        fixtures.put("list_8", l8);

        Map<String, String> hm = new LinkedHashMap<>();
        hm.put("color", "aquamarine");
        hm.put("model", "Beetle");
        fixtures.put("map_hm", hm);

        List<Object> nested = new ArrayList<>();
        nested.add(new ArrayList<>(Arrays.asList(0, 1)));
        nested.add(2);
        fixtures.put("nested_list", nested);

        for (Map.Entry<String, Object> e : fixtures.entrySet()) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            Hessian2Output out = new Hessian2Output(bos);
            out.writeObject(e.getValue());
            out.flush();
            System.out.println(e.getKey() + "\u0007" + toHex(bos.toByteArray()));
        }
    }

    // ------------------------------------------------------------------
    // decode：hex → 对象，规范形式输出
    // ------------------------------------------------------------------

    private static Object decode(byte[] bs) throws Exception {
        Hessian2Input in = new Hessian2Input(new ByteArrayInputStream(bs));
        return in.readObject();
    }

    @SuppressWarnings("unchecked")
    private static String render(Object o) throws Exception {
        PrintStream sb = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        return renderInto(o);
    }

    @SuppressWarnings("unchecked")
    private static String renderInto(Object o) throws Exception {
        if (o == null) {
            return "null";
        } else if (o instanceof String) {
            return '"' + (String) o + '"';
        } else if (o instanceof Byte || o instanceof Short || o instanceof Integer || o instanceof Long) {
            return String.valueOf(o);
        } else if (o instanceof Float || o instanceof Double) {
            return String.valueOf(o);
        } else if (o instanceof Boolean) {
            return String.valueOf(o);
        } else if (o instanceof Date) {
            return "Date(" + ((Date) o).getTime() + ")";
        } else if (o instanceof byte[]) {
            return "bytes[" + toHex((byte[]) o) + "]";
        } else if (o instanceof Map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<Object, Object> e : ((Map<Object, Object>) o).entrySet()) {
                if (!first) sb.append(", ");
                first = false;
                sb.append(renderInto(e.getKey())).append(": ").append(renderInto(e.getValue()));
            }
            return sb.append("}").toString();
        } else if (o instanceof Collection || o instanceof Object[]) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            if (o instanceof Collection) {
                for (Object x : (Collection<Object>) o) {
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append(renderInto(x));
                }
            } else {
                for (Object x : (Object[]) o) {
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append(renderInto(x));
                }
            }
            return sb.append("]").toString();
        } else {
            // POJO：类名(字段=值, ...)
            StringBuilder sb = new StringBuilder(o.getClass().getSimpleName()).append("(");
            boolean first = true;
            for (Field f : o.getClass().getDeclaredFields()) {
                f.setAccessible(true);
                if (!first) sb.append(", ");
                first = false;
                sb.append(f.getName()).append("=").append(renderInto(f.get(o)));
            }
            return sb.append(")").toString();
        }
    }

    // ------------------------------------------------------------------
    // util
    // ------------------------------------------------------------------

    private static String repeat(char c, int n) {
        char[] cs = new char[n];
        Arrays.fill(cs, c);
        return new String(cs);
    }

    private static String repeat(String s, int n) {
        StringBuilder b = new StringBuilder(s.length() * n);
        for (int i = 0; i < n; i++) b.append(s);
        return b.toString();
    }

    private static byte[] repeatBytes(int n, byte b) {
        byte[] bs = new byte[n];
        Arrays.fill(bs, b);
        return bs;
    }

    private static String toHex(byte[] bs) {
        StringBuilder s = new StringBuilder(bs.length * 2);
        for (byte b : bs) s.append(String.format("%02x", b));
        return s.toString();
    }

    private static byte[] hexToBytes(String hex) {
        int n = hex.length();
        byte[] bs = new byte[n / 2];
        for (int i = 0; i < n; i += 2) {
            bs[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
        }
        return bs;
    }
}