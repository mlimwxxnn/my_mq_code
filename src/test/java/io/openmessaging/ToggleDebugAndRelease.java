package io.openmessaging;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("ALL")
public class ToggleDebugAndRelease {
    private static final String SRC_DIR = "./src/main";
    private static final String COMMENT_FLAG = "// @";
    public static void main(String[] args) throws IOException {
        run();
    }

    private static void run(){
        List<File> srcFiles = getAllFiles(new File(SRC_DIR));
        Boolean debug = null;
        try {
            System.out.println("正在进行切换");
            for (File srcFile : srcFiles) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(srcFile));
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("tmpFile"));
                String line;
                while((line = bufferedReader.readLine()) != null){
                    if(line.contains(COMMENT_FLAG)){
                        if(line.charAt(0) == '/'){
                            line = line.substring(2); // 去掉注释
                        } else {
                            line = "//" + line;  // 加上注释
                        }
                    }
                    bufferedWriter.write(line + "\r\n");
                }
                bufferedReader.close();
                bufferedWriter.close();
                FileChannel dstChannel = FileChannel.open(srcFile.toPath(), StandardOpenOption.WRITE);
                FileChannel srcChannel = FileChannel.open(Paths.get("tmpFile"), StandardOpenOption.READ);
                dstChannel.truncate(0);
                srcChannel.transferTo(0, srcChannel.size(), dstChannel);
                new File("tmpFile").delete();
            }
            System.out.println("切换完成");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static List<File> getAllFiles(File file){
        ArrayList<File> fileList = new ArrayList<>();
        if(file.isFile()){
            fileList.add(file);
        } else {
            for (File listFile : Objects.requireNonNull(file.listFiles())) {
                if(listFile.isFile()){
                    fileList.add(listFile);
                } else {
                    fileList.addAll(getAllFiles(listFile));
                }
            }
        }
        return fileList;
    }



}
