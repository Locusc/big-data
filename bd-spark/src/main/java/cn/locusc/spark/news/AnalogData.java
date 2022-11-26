package cn.locusc.spark.news;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * 模拟数据变化
 * 主要是从输入目录读取数据到输出目录(增加延时)
 */
public class AnalogData {

    public static void main(String[] args){
        String inputFile = args[0];
        String outputFile = args[1];
        try {
            readData(inputFile,outputFile);
        }catch(Exception ignored){ }
    }

    /**
     * 读取文件数据
     */
    public static void readData(String inputFile,String outputFile) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String tmp = null;
        try {
            fis = new FileInputStream(inputFile);
            isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            br = new BufferedReader(isr);
            //计数器
            int counter=1;
            //按行读取文件数据
            while ((tmp = br.readLine()) != null) {
                //打印输出读取的数据
                System.out.println("第"+counter+"行："+tmp);
                //数据写入文件
                writeData(outputFile,tmp);
                counter++;
                //方便观察效果，控制数据参数速度
                Thread.sleep(1000);
            }
            isr.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * 文件写入数据
     */
    public static void writeData(String outputFile, String line) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile, true)));
            out.write("\n");
            out.write(line);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
