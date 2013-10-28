package ru.fizteh.fivt.students.mikhaylova_daria.db;

import java.io.*;
import java.util.HashMap;
import java.util.Vector;
import java.util.zip.DataFormatException;

public class FileMap {
    HashMap<String, String> fileMap = new HashMap<String, String>();
    File file;
    Short[] id;
    Boolean isLoaded;

    FileMap() {

    }

    FileMap(File file, Short[] id) {
        this.file = file;
        this.id = id;
        isLoaded = false;
    }

    public void put(String[] arg) throws IOException, DataFormatException {
        if (!isLoaded) {
            isLoaded = true;
            readerFile();
        }
        if (arg.length != 2) {
            throw new IllegalArgumentException("put: Wrong number of arguments");
        }
        arg[1] = arg[1].trim();
        arg = arg[1].split("\\s+", 2);
        if (arg.length != 2) {
            throw new IllegalArgumentException("put: Wrong number of arguments");
        }
        if (!fileMap.containsKey(arg[0])) {
             System.out.println("new");
        }
        if (fileMap.containsKey(arg[0])) {
            if (fileMap.containsKey(arg[0])) {
                System.out.println("overwrite\n" + fileMap.get(arg[0]));
            }
        }
        fileMap.put(arg[0], arg[1]);
        try {
            this.writerFile();
        } catch (Exception e) {
            throw new IOException("Writing error", e); //!!!!
            //System.err.println();
            //System.exit(1);
        }
    }

    public void get(String[] arg) throws IOException, DataFormatException {
        if (!isLoaded) {
            isLoaded = true;
            readerFile();
        }
        if (arg.length != 2) {
            throw new IllegalArgumentException("get: Wrong number of arguments");
        }
        arg[1] = arg[1].trim();
        arg = arg[1].split("\\s+");
        if (arg.length != 1) {
            throw new IllegalArgumentException("get: Wrong number of arguments");
        }
        if (fileMap.containsKey(arg[0])) {
            System.out.println("found\n" + fileMap.get(arg[0]));
        } else {
            System.out.println("not found");
        }
    }

    public void remove(String[] arg) throws  IOException, DataFormatException {
        if (!isLoaded) {
            isLoaded = true;
            readerFile();
        }
        if (arg.length != 2) {
            throw new IllegalArgumentException("remove: Wrong number of arguments");
        }
        arg[1] = arg[1].trim();
        arg = arg[1].split("\\s+");
        if (arg.length != 1) {
            throw new IllegalArgumentException("remove: Wrong number of arguments");
        }
        if (fileMap.containsKey(arg[0])) {
            fileMap.remove(arg[0]);
            System.out.println("removed");
        } else {
            System.out.println("not found");
        }
        try {
            writerFile();
        } catch (Exception e) {
            throw new IOException("Writing error", e);//!!!!!!
            //System.err.println("Writing error");
            //System.exit(1);
        }
        if (file.length() == 0) {
            deleteEmptyFile();
        }
    }

    public void exit(String[] arg) throws IOException { ///??? что с этим делать?
        try {
            writerFile();
        } catch (Exception e) {
            throw new IOException("Writing error", e);//!!!!!!
            //System.err.println(e.getMessage());
            //System.exit(1);
        }
        System.exit(0);
    }

    private void writerFile() throws IOException {
        RandomAccessFile fileDateBase = null;
        try {
            fileDateBase = new RandomAccessFile(file, "rw");
            fileDateBase.setLength(0);
        } catch (Exception e) {
            throw new IOException("Writing error", e);//!!!!!!
            //System.err.println("Unknown error");
            //System.exit(1);
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
            //System.err.println("Unknown error");
            fileDateBase.close();
            throw new IOException("Writing error", e);//!!!!!!
            //System.exit(1);
        }
        fileDateBase.close();
    }

    private boolean deleteEmptyFile() {
        //if (!file.delete()) {
            //System.err.println("Deleting of file error");
            //System.exit(1);
        //}
        isLoaded = false;
        fileMap.clear();
        return file.delete();
    }

    void setAside() {
        if (isLoaded) {
            fileMap.clear();
            isLoaded = false;
        }
    }

    void readerFile() throws IOException, DataFormatException {
        RandomAccessFile dateBase = null;
        try {
            dateBase = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            return;
        } catch (Exception e) {
            throw new IOException(file.getName() + ": Reading error", e);//!!!!!!
            //System.err.println(file.toPath().toString() + ": Opening isn't possible");
            //System.exit(1);
        }
        try {
            if (dateBase.length() == 0) {
                dateBase.close();
                deleteEmptyFile();
                return;
            }
        } catch (Exception e) {
            throw new IOException(file.getName() + ": Reading error", e);//!!!!!!
            //System.err.println("Unknown error");
            //System.exit(1);
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
                //System.err.println("Bad file");
                dateBase.close();
                throw new DataFormatException(file.getName());//!!!!!!
                //System.exit(1);
            }
            offsetAndKeyMap.put(offset, key);
            final int firstOffset = offset;
            try {
                int lastOffset = offset;
                String lastKey = null;
                while (dateBase.getFilePointer() < firstOffset) {
                    lastKey = key;
                    key = readKey(dateBase);
                    lastOffset = offset;
                    offset = dateBase.readInt();
                    offsetAndKeyMap.put(offset, key);
                    keyAndValueLength.put(lastKey, offset - lastOffset);
                    if (keyAndValueLength.containsKey(key)) {
                      //  System.err.println("Bad dates");
                        dateBase.close();
                        throw new DataFormatException(file.getName() +": " + key +": The key is already contained");//!!
                       // System.exit(1);
                    }
                }
                keyAndValueLength.put(key, (int) dateBase.length() - offset);
            } catch (EOFException e) {
                //System.err.println("Bad file");
                dateBase.close();
                throw new DataFormatException(file.getName());//!!!!!!
                //System.exit(1);
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
               // System.err.println("Bad File");
                dateBase.close();
                throw new DataFormatException(file.getName());//!!!!!!
                //System.exit(1);
            }
        } catch (Exception e) {
            //System.err.println("Unknown error");
            try {
                dateBase.close();
            } catch (Exception e2) {
                throw new IOException(file.getName(), e2);//!!!!!!
                //System.err.println("Unknown error");
               // System.exit(1);
            }
            throw new IOException(file.getName(), e); //!!!!!!
            //System.exit(1);
        }
        try {
            dateBase.close();
        } catch (Exception e) {
            throw new IOException(file.getName(), e);//!!!!!!!
            //System.err.println("Unknown error");
            //System.exit(1);
        }
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
            //System.err.println("Bad file");
            dateBase.close();
            throw new DataFormatException(file.getName());//!!!
            //System.exit(1);
        }
        String key = null;
        try {
            byte[] keyInBytes = new byte[keyBuilder.size()];
            for (int i = 0; i < keyBuilder.size(); ++i) {
                keyInBytes[i] = keyBuilder.elementAt(i);
            }
            key = new String(keyInBytes, "UTF8");
        } catch (Exception e) {
            //System.err.println("Reading Error");
            dateBase.close();
            throw  new IOException(file.getName(), e);
            //System.exit(1);
        }
        return key;
    }
}


