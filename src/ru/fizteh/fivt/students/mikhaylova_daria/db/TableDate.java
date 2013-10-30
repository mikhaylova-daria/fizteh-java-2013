package ru.fizteh.fivt.students.mikhaylova_daria.db;


import java.io.File;
import ru.fizteh.fivt.storage.strings.*;

public class TableDate implements Table {

    File tableFile;
    DirDateBase[] dirArray = new DirDateBase[16];

    TableDate(File tableFile) {
        this.tableFile = tableFile;
        if (!tableFile.exists()) {
            if (!tableFile.mkdir()) {
                tableFile = null;
            }
        }
        if (tableFile != null) {
            for (short i = 0; i < 16; ++i) {
                File dir = new File(tableFile.toPath().resolve(i + ".dir").toString());
                dirArray[i] = new DirDateBase(dir, i);
            }
        }
    }

    public String getName() {
        return tableFile.getName();
    }

    public String put(String key, String value) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        byte b = key.getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = (b / 16) % 16;
        return dirArray[nDirectory].fileArray[nFile].put(key, value);
    }

    public String remove(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        byte b = key.getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = b / 16 % 16;
        String removedValue;
        try {
            dirArray[nDirectory].startWorking();
            removedValue = dirArray[nDirectory].fileArray[nFile].remove(key);
            dirArray[nDirectory].deleteEmptyDir();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return removedValue;
    }

    public String get(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        byte b = key.getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = (b / 16) % 16;
        String getValue;
        try {
            dirArray[nDirectory].startWorking();
            getValue = dirArray[nDirectory].fileArray[nFile].get(key);
            dirArray[nDirectory].deleteEmptyDir();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return getValue;
    }

    int countChanges() {
        int numberOfChanges = 0;
        for (int i = 0; i < 16; ++i) {
            numberOfChanges += dirArray[i].countChanges();
        }
        return numberOfChanges;
    }

    public int size() {
        int numberOfKeys = 0;
        for (int i = 0; i < 16; ++i) {
            numberOfKeys += dirArray[i].size();
        }
        return numberOfKeys;
    }

    public int commit() {
        int numberOfChanges = 0;
        for (int i = 0; i < 16; ++i) {
            numberOfChanges += dirArray[i].commit();
        }
        return numberOfChanges;
    }

    public int rollback() {
        int numberOfChanges = 0;
        for (int i = 0; i < 16; ++i) {
            numberOfChanges += dirArray[i].rollback();
        }
        return numberOfChanges;
    }

}
