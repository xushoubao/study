
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;

/**
 * created by kelvin on 2019/08/13
 */
public class FileUtils {

    /**
     * Base64转文件
     *
     * @param imgStr      base64
     * @param imgFilePath 输出路径
     * @return
     */
    public static boolean Base64ToFile(String imgStr, String imgFilePath) {
        if (imgStr == null) {
            return false;
        }
        if (imgStr.contains(",")) {
            imgStr = imgStr.substring(imgStr.indexOf(",") + 1, imgStr.length());
        }
        try {
            byte[] b = Base64.getDecoder().decode(imgStr);
            for (int i = 0; i < b.length; ++i) {
                if (b[i] < 0) {
                    b[i] += 256;
                }
            }

            OutputStream out = new FileOutputStream(imgFilePath);
            out.write(b);
            out.flush();
            out.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 文件转base64
     * @param filePath
     * @return
     */
    public static String FileToBase64(String filePath) {
        if (filePath == null) {
            return null;
        }
        try {
            byte[] b = Files.readAllBytes(Paths.get(filePath));
            return Base64.getEncoder().encodeToString(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 读取文件内容
     * @param file
     */
    public static String fileToString(File file) {
        BufferedReader reader = null;
        String laststr = "";
        try {
            FileInputStream inputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
            reader = new BufferedReader(inputStreamReader);
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                laststr += tempString;
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return laststr;
    }


    /**
     * 向文件写入数据（字符）
     * @param filepath
     * @param data
     */
    public static void charToFile(String filepath, String fileName, String data) {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(filepath));
            out.write(data);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 向文件写入数据（字节）
     * @param filePath
     * @param data
     */
    public static void byteToFile(String filePath, String fileName, String data) {
        File file = new File(filePath);
        if (!file.exists()) {
            file.mkdirs();
        }
        File newFile = new File(filePath + fileName);
        FileOutputStream out = null;
        try {
            if (!newFile.exists()) {
                newFile.createNewFile();
            }
            out = new FileOutputStream(newFile);
            out.write(data.getBytes());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 批量重命名文件，用逗号分隔多个文件路径(移动文件)
     * 文件名不能带逗号
     * @param oldPath
     * @param newPath
     */
    public static void renameFiles(String oldPath, String newPath) {
        if (StringUtils.isBlank(oldPath) || StringUtils.isBlank(newPath)) {
            return;
        }
        String[] path = oldPath.split(",");
        String[] dest = newPath.split(",");
        for (int i = 0; i<path.length; i++) {
            File file = new File(path[i]);
            File destFile = new File(dest[i]);
            File mdir = new File(destFile.getParentFile().getAbsolutePath());
            if (!file.exists()) {
                return;
            }
            if (!mdir.exists()) {
                mdir.mkdirs();
            }
            if (destFile.exists()) {
                destFile.delete();
            }
            file.renameTo(destFile);
        }
    }


    /**
     *判断IP  是否在文件夹内
     * */
    public static String findStringInFile(String str) throws IOException{
        File file = new File("/data/www/mnfs/vrapp/iplist.txt");
        InputStreamReader read = new InputStreamReader(new FileInputStream(file),"UTF-8");
        BufferedReader bufferedReader = new BufferedReader(read);
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            if(line.startsWith("#")){
                continue;
            }
            //指定字符串判断处
            if (line.contains(str)) {
                return line;
            }
        }
        return line;
    }


    public static void main(String[] args) throws IOException {

        System.out.println(FileToBase64("/Users/bao/Downloads/sccy.jpg"));
        // File files = file.getParentFile();
        // System.out.println(file.getAbsolutePath());


        // File dest = new File("D:\\oo.txt");
        // Files.copy(file.toPath(), dest.toPath());

        // writeToFileByte("D:\\oo\\","oo.txt", "这是一段话");
        // Base64ToImage("7QN7IkdyYXBoTm9kZUxpc3QiOlt7IlBvcyI6eyJ4IjotMC43MDQ5OTk5ODMzMTA2OTk1LCJ5IjotMS4zMDU5OTk5OTQyNzc5NTR9LCJJbmRleCI6MSwiTmV4dE5vZGVMaXN0IjpbMiw0XX0seyJQb3MiOnsieCI6LTAuNzA0OTk5OTgzMzEwNjk5NSwieSI6MS41OTA5OTk5NjA4OTkzNTN9LCJJbmRleCI6MiwiTmV4dE5vZGVMaXN0IjpbMSwzXX0seyJQb3MiOnsieCI6Mi45NzE5OTk4ODM2NTE3MzM2LCJ5IjoxLjU5MDk5OTk2MDg5OTM1M30sIkluZGV4IjozLCJOZXh0Tm9kZUxpc3QiOlsyLDRdfSx7IlBvcyI6eyJ4IjoyLjk3MTk5OTg4MzY1MTczMzYsInkiOi0xLjMwNTk5OTk5NDI3Nzk1NH0sIkluZGV4Ijo0LCJOZXh0Tm9kZUxpc3QiOlszLDFdfV0sIlNlZ21lbnRMaXN0IjpbeyJQMCI6MSwiUDEiOjJ9LHsiUDAiOjIsIlAxIjozfSx7IlAwIjozLCJQMSI6NH0seyJQMCI6NCwiUDEiOjF9XSwiUmVnaW9uTGlzdCI6W3siSXRlbTEiOlsxLDIsMyw0XSwiSXRlbTIiOltdfV19E3sicm9vbVRpbGVMaXN0IjpbXX0OAAAATQQAAAAAgD8EbnVsbMh6GcAAAAC/NYgKQAAAAAAAAAAAAACAvwAAgD8AAIA/AACAPwAAAAAOAAAATQQAAAAAgD8EbnVsbNtNIT8AAAAALevnPgEAgDMAAAAAAACAvwAAgD8AAIA/AACAPwAAAAAKAAAAhgMAAAAAwD/NzEw+zcxMPgEAwD/NzEw/AAAAAAAAAAAAAAAAAAAAAAAAAAAIMTM1X05OTk4EbnVsbARudWxsBG51bGwAAAAAAAAAAAAAAAABAAAAABh2XzE1NTcyMTkzMTI4MDBfZmlsZS5kYXQAAAAAAAAAAAAAAAABAAAAABh2XzE1NTcyMTkzMTI4MDBfZmlsZS5kYXQ1XgpAAAAAAJ/vRz8AAIA/AAAAALbDh7MAAIA/AACAPwAAgD8AAAAA", "D:\\cc.dat");
    }
}