package ru.fizteh.fivt.students.ermolenko.shell;

import java.io.File;
import java.io.IOException;

public class ShellMain {
    public static void main(String[] args) throws IOException {
        File currentDirectory = new File("");
        Shell shell = new Shell(currentDirectory);

        if (args.length != 0) {
            //в args лежат слова разделенные пробелами
            shell.batchState(shell, args);
        } else {
            shell.interactiveState();
        }
        System.exit(0);
    }
}