package com.oyo.price.bean.bo;

import com.opencsv.CSVWriter;
import sun.misc.BASE64Encoder;
import top.rdfa.framework.utils.collection.Lists;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author fh
 */
public class LibraryCrawlers {

    private static volatile BufferedReader in;

    /**
     * 向指定URL发送GET方法的请求
     *
     * @param url   发送请求的URL
     * @param param 请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return URL  所代表远程资源的响应结果
     */
    public static String sendGet(String url, String param) {
        String result = "";

        try {
            String urlNameString = url + "?" + param;
            URL realUrl = new URL(urlNameString);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();

            BufferedReader in = getBufferedReader(connection);
            System.out.println(Thread.currentThread().getId() + "远程调用完成>>>>>");
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }// 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    private static BufferedReader getBufferedReader(URLConnection connection) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(
                connection.getInputStream()));
        return in;
    }

    public static void main(String[] args) {

        String url = "http://202.96.31.78/xlsworkbench/publish";
        String partParams = "keyWord=+&orderProperty=PU_CHA_BIAN_HAO&orderWay=asc&";

        boolean ifMultiThreadHandle = false;
        if (ifMultiThreadHandle) {
            multiThreadHandle(url, partParams);
        } else {
            String saveFileName = "/Users/fh/Downloads/libraryCrawlers17.csv";
            Integer startPageNum = 80001;
            Integer pageEnd = 85000;
            mainThreadHandle(url, partParams, saveFileName, startPageNum, pageEnd);
        }


    }

    /**
     * 多线程处理
     *
     * @param url
     * @param partParams
     */
    private static void multiThreadHandle(String url, String partParams) {
        int perThreadHandlePageNum = 5000;
        int maxThreadNum = 16;
        ExecutorService executor = Executors.newFixedThreadPool(16);

        for (int i = 1; i <= maxThreadNum; i++) {
            String saveFileName = "/Users/fh/Downloads/libraryCrawlers" + i + ".csv";
            Integer startPageNum = (i - 1) * perThreadHandlePageNum + 1;
            Integer pageEnd = i * perThreadHandlePageNum;

            executor.execute(() -> mainFun(saveFileName, url, partParams, startPageNum, pageEnd));
            System.out.println("线程" + Thread.currentThread().getId() + "已提交,将处理:" + startPageNum + " ~ " + pageEnd + " 页数据");

        }
    }

    /**
     * 单线程处理
     *
     * @param url
     * @param partParams
     */
    private static void mainThreadHandle(String url, String partParams, String saveFileName, Integer startPageNum, Integer pageEnd) {

        int count = mainFun(saveFileName, url, partParams, startPageNum, pageEnd);
        System.out.println("============处理结束, 共提取" + count + "条数据===========!");
    }


    private static int mainFun(String saveFileName, String url, String partParams, Integer pageFrom, Integer pageTo) {

        String htmlStr = null;
        List<String[]> allLineList = Lists.newArrayList();
        List<String> matchList;
        int count = 0;
        while (true) {
            String allParam = partParams + base64(pageFrom);
            System.out.println(Thread.currentThread().getId() + "开始处理第" + pageFrom + "页数据............");
            htmlStr = LibraryCrawlers.sendGet(url, allParam);

            // 第一步过滤提取10条
            matchList = match(htmlStr);
            count += matchList.size();

            String lineTitle = "以下为第" + pageFrom + "页数据";

            if (matchList.size() > 0) {
                allLineList.add(new String[]{lineTitle});
                for (String s : matchList) {
                    // 第二步过滤提取关键字
                    allLineList.add(extract(s));
                }
            } else {
                break;
            }

            // 保存
            if (allLineList.size() > 0) {
                writeToFile(saveFileName, allLineList);
                System.out.println(pageFrom + "保存完成!");
            }

            if (matchList.size() < 10) {
                break;
            }
            if (pageFrom.equals(pageTo)) {
                break;
            }

            pageFrom++;
            allLineList.clear();
            matchList.clear();
        }
        return count;
    }


    /**
     * 获取指定HTML标签的指定属性的值
     *
     * @param source 要匹配的源文本
     * @return 属性值列表
     */
    public static List<String> match(String source) {

        List<String> result = new ArrayList<String>();
        String resultStr = null;
        String reg = "<div class=\"item\">.+<div style=\"padding-left:40px;\">";
        Matcher m = Pattern.compile(reg).matcher(source);
        if (m.find()) {
            resultStr = m.group(0);
        }
        if (resultStr != null) {
            String[] arr = resultStr.split("<div class=\"item\">");

            if (arr.length > 0) {
                result = new ArrayList<>(Arrays.asList(arr));
                result.remove(0);
            }
        }
        return result;
    }

    /**
     * 提取关键字
     *
     * @param text
     * @return
     */
    public static String[] extract(String text) {
        String[] eleList = new String[]{};
        String newChar = ",//";

        text = text.replace("\"", "").replace(" ", "");

        String reg1 = "<pclass=ot><spantitle=普查編號style=padding:0;data-field=PUCHABIANHAO>";
        String reg2 = "</span><spantitle=索書號data-field=SUOSHUHAO>";
        String reg3 = "</span></p><pclass=tmzz><spantitle=題名著者class=tmdata-field=TIMING>";
        String reg4 = "</span><spantitle=索書號data-field=SUOSHUHAO>00009</span></p><pclass=tmzz><spantitle=題名著者class=tmdata-field=TIMING>";
        String reg5 = "</span></p><divstyle=padding-left:20px;><pclass=bb><spantitle=版本data-field=BANBEN>";
        String reg6 = "</span><spantitle=批校題跋data-field=PIJIAOTIBA></span></p><pclass=blk><spantitle=册（件）数data-field=CESHU>";
        String reg7 = "</span><spantitle=裝幀形式data-field=ZHUANGZHENXINGSHI></span><spantitle=版式data-field=BANSHI>";
        String reg8 = "</span><spantitle=单位data-field=DANWEI>";
        String reg9 = "</span></p></div><divstyle=padding-left:40px;></div></div>";

        String tempStr = text.replace(reg1, newChar).replace(reg2, newChar).replace(reg3, newChar).replace(reg4, newChar).replace(reg5, newChar).replace(reg6, newChar).replace(reg7, newChar).replace(reg8, newChar).replace(reg9, newChar);

        // 对每组最后一个做特殊处理
        String reg20 = "</span></p></div><divstyle=padding-left:40px;>";
        String reg21 = "</span></p><pclass=ot><spantitle=存卷data-field=CUNJUAN>";
        tempStr = tempStr.replace(reg20, "").replace(reg21, "");

        eleList = tempStr.split("//");
        eleList = Arrays.copyOfRange(eleList, 1, eleList.length);

        return eleList;
    }



    /**
     * 编码
     */
    public static String base64(Integer pageNum) {
        BASE64Encoder encoder = new BASE64Encoder();
        String text = "page=" + pageNum.toString();
        byte[] textByte = new byte[0];
        try {
            textByte = text.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encoder.encode(textByte);
    }


    /**
     * 保存文件
     *
     * @param fileName
     * @param myEntries
     * @throws IOException
     */
    public static void writeToFile(String fileName, List<String[]> myEntries) {

        File file = new File(fileName);
        try {
            if (!file.exists()) {
                boolean r = file.createNewFile();
                System.out.println("创建文件结果:" + r);
            }

            try (CSVWriter writer = new CSVWriter(new FileWriter(fileName, true), '\t', CSVWriter.NO_QUOTE_CHARACTER)) {
                for (String[] row : myEntries) {
                    writer.writeNext(row);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
