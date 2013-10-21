package ru.fizteh.fivt.students.mikhaylova_daria.db;


import java.io.File;
import java.io.IOException;

public class TableDate {
    File tableFile;
    DirDateBase[] dirArray = new DirDateBase[16];
    TableDate(File tableFile) {
        this.tableFile = tableFile;
        if (!tableFile.exists()) {
            if (!tableFile.mkdir()) {
                System.err.println("Unknown error");
                System.exit(1);
            } else {
                System.out.println("created");
            }
        }
        for (short i = 0; i < 16; ++i) {
            File dir = new File(tableFile.toPath().resolve(i + ".dir").toString());
            dirArray[i] = new DirDateBase(dir, i);
        }
    }

    void put(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("put: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+", 2);
        if (arg.length != 2) {
            throw new IOException("put: Wrong number of arguments");
        }
        byte b = arg[0].getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = (b / 16) % 16;
        dirArray[nDirectory].startWorking();
        dirArray[nDirectory].fileArray[nFile].put(command);
    }

    void remove(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("remove: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IOException("remove: Wrong number of arguments");
        }
        byte b = arg[0].getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = b / 16 % 16;
        dirArray[nDirectory].startWorking();
        dirArray[nDirectory].fileArray[nFile].remove(command);
        dirArray[nDirectory].deleteEmptyDir();
    }

    void get(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("get: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IOException("get: Wrong number of arguments");
        }
        byte b = arg[0].getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = (b / 16) % 16;
        dirArray[nDirectory].startWorking();
        dirArray[nDirectory].fileArray[nFile].get(command);
    }

}
