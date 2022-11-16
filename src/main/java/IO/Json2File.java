package IO;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Json2File {
    public static void main(String[] args) {


    }

    public static void writeJson2File(String outputPath,String json,Boolean isAppend){
        try {
            File f = new File(outputPath);
            BufferedWriter bw = new BufferedWriter(new FileWriter(f,isAppend));
            if (!f.exists()){
                f.createNewFile();
            }
            bw.append(json);
            //bw.write(json);
            bw.newLine();
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
