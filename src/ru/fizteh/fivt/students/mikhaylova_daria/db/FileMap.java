package ru.fizteh.fivt.students.mikhaylova_daria.db;

import java.io.*;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.zip.DataFormatException;

public class FileMap {
    private HashMap<String, String> fileMapInitial = new HashMap<String, String>();
    private HashMap<String, String> fileMap = new HashMap<String, String>();
    private File file;
    private int size = 0;
    private Short[] id;
    Boolean isLoaded = false;

    FileMap() {

    }

    FileMap(File file, Short[] id) {
        this.file = file;
        this.id = id;
    }

    public String put(String key, String value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null");
        }
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("value is null");
        }
        if (!isLoaded) {
            try {
                readerFile();
            } catch (IOException e) {
                throw new RuntimeException("Reading error", e);
            } catch (DataFormatException e) {
                throw new RuntimeException("Bad data", e);
            }
        }
        String overwritten = null;
        if (fileMap.containsKey(key)) {
            overwritten = fileMap.get(key);
        }
        fileMap.put(key, value);
        return overwritten;
    }

    public String get(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (!isLoaded) {
            try {
                readerFile();
            } catch (IOException e) {
                throw new RuntimeException("Reading error", e);
            } catch (DataFormatException e) {
                throw new RuntimeException("Bad data", e);
            }
        }
        String value = null;
        if (fileMap.containsKey(key)) {
            value = fileMap.get(key);
        }
        return value;
    }

    public String remove(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (!isLoaded) {
            try {
                readerFile();
            } catch (IOException e) {
                throw new RuntimeException("Reading error", e);
            } catch (DataFormatException e) {
                throw new RuntimeException("Bad data", e);
            }
        }
        String value = null;
        if (fileMap.containsKey(key)) {
            value = fileMap.get(key);
            fileMap.remove(key);
        }
        return value;
    }

    private void writerFile() throws IOException {
        RandomAccessFile fileDateBase = null;
            try {
                fileDateBase = new RandomAccessFile(file, "rw");
                fileDateBase.setLength(0);
            } catch (Exception e) {
                throw new IOException("Writing error", e);
            }
            try {
                HashMap<String, Long> offsets = new HashMap<String, Long>();
                long currentOffsetOfValue;
                long offset = fileDateBase.getFilePointer();
                for (String key: fileMap.keySet()) {
                    fileDateBase.write(key.getBytes("UTF8"));
                    fileDateBase.write("\0".getBytes());
                    offset = fileDateBase.getFilePointer();
                    offsets.put(key, offset);
                    fileDateBase.seek(fileDateBase.getFilePointer() + 4);
                    currentOffsetOfValue = fileDateBase.getFilePointer();
                }

                long currentPosition = 0;
                for (String key: fileMap.keySet()) {
                    fileDateBase.write(fileMap.get(key).getBytes("UTF8")); // выписали значение
                    currentPosition  = fileDateBase.getFilePointer();
                    currentOffsetOfValue = currentPosition - fileMap.get(key).getBytes("UTF8").length;
                    fileDateBase.seek(offsets.get(key));
                    Integer lastOffsetInt = new Long(currentOffsetOfValue).intValue();
                    fileDateBase.writeInt(lastOffsetInt);
                    fileDateBase.seek(currentPosition);
                }
            } catch (Exception e) {
                fileDateBase.close();
                throw new IOException("Writing error", e);
            }
            fileDateBase.close();
            if (file.length() == 0) {
                deleteEmptyFile();
            }
            size = fileMap.size();
        fileMapInitial.clear();
        for (String key: fileMap.keySet()) {
            fileMapInitial.put(key, fileMap.get(key));
        }
    }

    private boolean deleteEmptyFile() {
        isLoaded = false;
        fileMap.clear();
        return file.delete();
    }

    void setAside() {
        if (isLoaded) {
            fileMap.clear();
            fileMapInitial.clear();
            isLoaded = false;
        }
    }

    void readerFile() throws IOException, DataFormatException {
        RandomAccessFile dateBase;
        try {
            dateBase = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            return;
        } catch (Exception e) {
            throw new IOException(file.getName() + ": Reading error", e);
        }
        try {
            if (dateBase.length() == 0) {
                dateBase.close();
                deleteEmptyFile();
                return;
            }
        } catch (Exception e) {
            throw new IOException(file.getName() + ": Reading error", e);
        }
        try {
            HashMap<Integer, String> offsetAndKeyMap = new HashMap<Integer, String>();
            HashMap<String, Integer> keyAndValueLength = new HashMap<String, Integer>();
            String key = readKey(dateBase);
            byte b = key.getBytes()[0];
            if (b < 0) {
                b *= (-1);
            }
            if (id[0] != b % 16 && id[1] != b / 16 % 16) {
                System.err.println("Illegal key in file " + file.toPath().toString());
                dateBase.close();
                System.exit(1);
            }
            if (keyAndValueLength.containsKey(key)) {
                System.err.println("Bad dates");
                dateBase.close();
                System.exit(1);
            }
            Integer offset = 0;
            try {
                offset = dateBase.readInt();
            } catch (EOFException e) {
                dateBase.close();
                throw new DataFormatException(file.getName());
            }
            offsetAndKeyMap.put(offset, key);
            final int firstOffset = offset;
            try {
                int lastOffset = offset;
                String lastKey;
                while (dateBase.getFilePointer() < firstOffset) {
                    lastKey = key;
                    key = readKey(dateBase);
                    lastOffset = offset;
                    offset = dateBase.readInt();
                    offsetAndKeyMap.put(offset, key);
                    keyAndValueLength.put(lastKey, offset - lastOffset);
                    if (keyAndValueLength.containsKey(key)) {
                        dateBase.close();
                        throw new DataFormatException(file.getName() +": " + key +": The key is already contained");
                    }
                }
                keyAndValueLength.put(key, (int) dateBase.length() - offset);
            } catch (EOFException e) {
                dateBase.close();
                throw new DataFormatException(file.getName());
            }
            int lengthOfValue = 0;
            try {
                while (dateBase.getFilePointer() < dateBase.length()) {
                    int currentOffset = (int) dateBase.getFilePointer();
                    if (!offsetAndKeyMap.containsKey(currentOffset)) {
                        System.err.println("Bad file");
                        dateBase.close();
                        System.exit(1);
                    } else {
                        key = offsetAndKeyMap.get(currentOffset);
                        lengthOfValue = keyAndValueLength.get(key);
                    }
                    byte[] valueInBytes = new byte[lengthOfValue];
                    for (int i = 0; i < lengthOfValue; ++i) {
                        valueInBytes[i] = dateBase.readByte();
                    }
                    String value = new String(valueInBytes, "UTF8");
                    fileMap.put(key, value);
                }
            } catch (EOFException e) {
                dateBase.close();
                throw new DataFormatException(file.getName());
            }
        } catch (Exception e) {
            try {
                dateBase.close();
            } catch (Exception e2) {
                throw new IOException(file.getName(), e2);
            }
            throw new IOException(file.getName(), e);
        }
        try {
            dateBase.close();
        } catch (Exception e) {
            throw new IOException(file.getName(), e);
        }
        fileMapInitial.clear();
        for (String key: fileMap.keySet()) {
            fileMapInitial.put(key, fileMap.get(key));
        }
        size = fileMap.size();
        isLoaded = true;
    }

    private String readKey(RandomAccessFile dateBase) throws IOException, DataFormatException {
        Vector<Byte> keyBuilder = new Vector<Byte>();
        try {
            byte buf = dateBase.readByte();
            while (buf != "\0".getBytes("UTF8")[0]) {
                keyBuilder.add(buf);
                buf = dateBase.readByte();
            }
        } catch (EOFException e) {
            dateBase.close();
            throw new DataFormatException(file.getName());
        }
        String key = null;
        try {
            byte[] keyInBytes = new byte[keyBuilder.size()];
            for (int i = 0; i < keyBuilder.size(); ++i) {
                keyInBytes[i] = keyBuilder.elementAt(i);
            }
            key = new String(keyInBytes, "UTF8");
        } catch (Exception e) {
            dateBase.close();
            throw  new IOException(file.getName(), e);
        }
        return key;
    }

    int numberOfChangesCounter() {
        int numberOfChanges = 0;
        Set<String> newKeys = fileMap.keySet();
        Set<String> oldKeys = fileMapInitial.keySet();
        for (String key: newKeys) {
            if (oldKeys.contains(key)) {
                if (!fileMap.get(key).equals(fileMapInitial.get(key))) {
                    ++numberOfChanges;
                }
            } else {
                ++numberOfChanges;
            }
        }
        for (String key: oldKeys) {
            if (!newKeys.contains(key)) {
                ++numberOfChanges;
            }
        }
        return numberOfChanges;
    }


    void commit() {
        int numberOfChanges = numberOfChangesCounter();
        if (numberOfChanges !=0) {
                try {
                    writerFile();
                } catch(IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Writing error", e);
                }
        }
    }

    int rollback() {
        int numberOfChanges = numberOfChangesCounter();
        fileMap.clear();
        for (String key: fileMapInitial.keySet()) {
            fileMap.put(key, fileMapInitial.get(key));
        }
        return numberOfChanges;
    }

    int size() {
        if (!isLoaded) {
            try {
                readerFile();
            } catch (IOException e) {
                throw  new RuntimeException("reading error", e);
            } catch (DataFormatException e) {
                throw new RuntimeException("Bad dates", e);
            }
        }
        return size;
    }

}
