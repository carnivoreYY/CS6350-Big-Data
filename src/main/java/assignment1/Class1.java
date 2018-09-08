package assignment1;

import java.util.Scanner;


public class Class1 {


    // Reading data using readLine
    private String dir;

    public void setDir(){
        Scanner in = new Scanner(System.in);
        String host_ = in.nextLine();

        this.dir = host_;
    }
    public String getDir() {
        return dir;
    }

}
