package ru.fizteh.fivt.students.mikhaylova_daria.db;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ru.fizteh.fivt.storage.structured.*;

public class TableData implements Table {

    File tableFile;
    DirDataBase[] dirArray = new DirDataBase[16];
    private ArrayList<Class<?>> columnTypes;
    TableManager manager;

    TableData(File tableFile, List<Class<?>> columnTypes, TableManager manager) {
        if (columnTypes == null) {
            throw new IllegalArgumentException("list of column's types is null");
        }
        if (columnTypes.isEmpty()) {
            throw new IllegalArgumentException("list of column's types is empty");
        }
        this.manager = manager;
        this.columnTypes = new ArrayList(columnTypes);
        this.tableFile = tableFile;
        if (!tableFile.exists()) {
            if (!tableFile.mkdir()) {
                throw new IllegalArgumentException("Creating of " + tableFile.toString() + "error");
            }
        }
        HashMap<String, String> types = new HashMap<>();
        types.put("Integer", "int");
        types.put("Long", "long");
        types.put("Double", "double");
        types.put("Float", "float");
        types.put("Boolean", "boolean");
        types.put("String", "String");
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < columnTypes.size(); ++i) {
            str = str.append(types.get(columnTypes.get(i).getSimpleName()));
            str = str.append(" ");
        }
        File sign = new File(tableFile, "signature.tsv");
        try {
            if (!sign.createNewFile()) {
                throw new IllegalArgumentException("Creating \"signature.tsv\" error");
            }
        } catch (IOException e) {
             throw new RuntimeException(e.getMessage(), e);
        }
        try (BufferedWriter signatureWriter =
                     new BufferedWriter(new FileWriter(sign))) {
            signatureWriter.write(str.toString());
        } catch (IOException e) {
            throw new IllegalArgumentException("Reading error: signature.tsv", e);
        }
        if (tableFile != null) {
            for (short i = 0; i < 16; ++i) {
                File dir = new File(tableFile.toPath().resolve(i + ".dir").toString());
                dirArray[i] = new DirDataBase(dir, i, this);
            }
        }
    }



    TableData(File tableFile, TableManager manager) {
        this.manager = manager;
        this.columnTypes = new ArrayList<>();
        this.tableFile = tableFile;
        String signature;
        File sign = new File(tableFile, "signature.tsv");
        if (!sign.exists()) {
            throw new IllegalArgumentException(sign.getName() + " does not exist");
        }
        try (BufferedReader signatureReader =
                     new BufferedReader(new FileReader(sign))) {
            signature = signatureReader.readLine();
        } catch (IOException e) {
            throw new IllegalArgumentException("Reading error: signature.tsv", e);
        }
        String[] signatures = signature.trim().split(" ");
        if (signatures.length == 0) {
            throw new IllegalArgumentException(sign.toString() + " Empty type list");
        }
        for (int i = 0; i < signatures.length; ++i) {
            if (signatures[i].equals("int")) {
                columnTypes.add(Integer.class);
            } else {
                if (signatures[i].equals("long")) {
                    columnTypes.add(Long.class);
                }  else {
                    if (signatures[i].equals("byte")) {
                        columnTypes.add(Byte.class);
                    } else {
                        if (signatures[i].equals("float")) {
                            columnTypes.add(Float.class);
                        } else {
                            if (signatures[i].equals("double")) {
                                columnTypes.add(Double.class);
                            } else {
                                if (signatures[i].equals("boolean")) {
                                    columnTypes.add(Boolean.class);
                                } else {
                                    if (signatures[i].equals("String")) {
                                        columnTypes.add(String.class);
                                    } else {
                                        throw new IllegalArgumentException("This type is not supposed: "
                                                + signatures[i]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for (short i = 0; i < 16; ++i) {
            File dir = new File(tableFile.toPath().resolve(i + ".dir").toString());
            dirArray[i] = new DirDataBase(dir, i, this);
        }

    }

    public String getName() {
        return tableFile.getName();
    }

    public Storeable put(String key, Storeable value) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (key.contains(" ") || key.contains("\n") || key.contains("\t")) {
            throw new IllegalArgumentException("Bad char in key");
        }
        if (key.isEmpty()) {
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
        return dirArray[nDirectory].fileArray[nFile].put(key, value, this);
    }

    public Storeable remove(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (key.contains(" ") || key.contains("\n") || key.contains("\t")) {
            throw new IllegalArgumentException("Bad char in key");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key is empty");
        }
        byte b = key.getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = b / 16 % 16;
        Storeable removedValue;
        try {
            dirArray[nDirectory].startWorking();
            removedValue = dirArray[nDirectory].fileArray[nFile].remove(key, this);
            dirArray[nDirectory].deleteEmptyDir();
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        return removedValue;
    }

    public Storeable get(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (key.contains(" ") || key.contains("\n") || key.contains("\t")) {
            throw new IllegalArgumentException("Bad char in key");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key is empty");
        }
        byte b = key.getBytes()[0];
        if (b < 0) {
            b *= (-1);
        }
        int nDirectory = b % 16;
        int nFile = (b / 16) % 16;
        Storeable getValue;
        try {
            dirArray[nDirectory].startWorking();
            getValue = dirArray[nDirectory].fileArray[nFile].get(key, this);
            dirArray[nDirectory].deleteEmptyDir();
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
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

    public int getColumnsCount() {
        return columnTypes.size();
    }

    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
        if (columnIndex >= columnTypes.size()) {
            throw new IndexOutOfBoundsException("Index " + columnIndex
                    + "does not exist. Number of columns is" + columnTypes.size());
        }
         return columnTypes.get(columnIndex);
    }

}