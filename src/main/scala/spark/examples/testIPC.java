package spark.examples;


import java.io.*;
import java.util.Scanner;

public class testIPC {
    public static void main(String [] args){
        try {
            //BufferedReader br = new BufferedReader(new FileReader("/proc/"+args[0]+"/fd/1"));
            Scanner  in = new Scanner( (new FileReader("/proc/"+args[0]+"/fd/1")) ); //read from svm_classify stdout
            FileWriter fw = new FileWriter("/proc/"+args[0]+"/fd/0");
            fw.write("0  |BT| (ROOT (S (NP (NNP Video) (NNS game)) (VP (VBP have) (VP (VBN be) (VP (VBN study) (PP (IN for) (NP (NNS link))) (PP (TO to) (NP (NN addiction) (CC and) (NN aggression)))))) (. .))) |ET|\t1:-0.01727909958824434 27761:0.24706527126233627 29179:0.23239099967436283 29330:0.19060365763664244 29689:0.010413015050477311 30612:0.05740979999831072 35726:0.046391699845072534 36083:0.13278110783458724 36805:0.06956725648734345 39042:0.18036855299820886 46693:0.12522188942404472 47677:0.01282324787327792  |EV|\n");
            fw.flush();
           /*if (in.hasNextLine())
               System.out.println("Line: "+in.nextLine());
           else
               System.out.println("No next line");*/

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
