package ru.fizteh.fivt.students.mikhaylova_daria.db;

import java.io.*;
import java.util.HashMap;


import ru.fizteh.fivt.students.mikhaylova_daria.shell.Parser;
import ru.fizteh.fivt.students.mikhaylova_daria.shell.Shell;

public class DbMain {

    private static HashMap<String, TableDate> bidDateBase = new HashMap<String, TableDate>();
    private static File mainDir;
    private static TableDate currentTable = null;

    public static void main(String[] arg) {
        String workingDirectoryName = System.getProperty("fizteh.db.dir");

        if (workingDirectoryName == null) {
            System.err.println("Property not found");
            System.exit(1);
        }

        mainDir = new File(workingDirectoryName);

        if (!mainDir.exists()) {
            System.err.println(workingDirectoryName + " doesn't exist");
            System.exit(1);
        }

        if (!mainDir.isDirectory()) {
            System.err.println(workingDirectoryName + " is not a directory");
            System.exit(1);
        }
        try {
            cleaner();
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unknown error");
            System.exit(1);
        }

        HashMap<String, String> commandsList = new HashMap<String, String>();
        commandsList.put("put", "put");
        commandsList.put("get", "get");
        commandsList.put("remove", "remove");
        commandsList.put("exit", "exit");
        commandsList.put("create", "create");
        commandsList.put("use", "use");
        commandsList.put("drop", "drop");
        try {
            try {
                Parser.parser(arg, DbMain.class, commandsList);
            } catch (Exception e) {
                System.err.println(e.toString());
            }
        } catch (Exception e) {
            System.err.println(e.toString());
            System.exit(1);
        }
    }
    private static void cleaner() throws Exception {
        HashMap<String, Short> fileNames = new HashMap<String, Short>();
        HashMap<String, Short> dirNames = new HashMap<String, Short>();
        for (short i = 0; i < 16; ++i) {
            fileNames.put(i + ".dat", i);
            dirNames.put(i + ".dir", i);
        }
        File[] tables = mainDir.listFiles();
        for (short i = 0; i < tables.length; ++i) {
            if (tables[i].isFile()) {
                throw new IOException(tables[i].toString() + " is not table");
            }
            File[] directories = tables[i].listFiles();
            if (directories.length > 16) {
                throw new IOException(tables[i].toString() + ": Wrong number of files in the table");
            }
            Short[] idFile = new Short[2];
            for (short j = 0; j < directories.length; ++j) {
                if (directories[j].isFile() || !dirNames.containsKey(directories[j].getName())) {
                    throw new IOException(directories[j].toString() + " is not directory of table");
                }
                idFile[0] = dirNames.get(directories[j].getName());
                File[] files = directories[j].listFiles();
                if (files.length > 16) {
                    throw new IOException(tables[i].toString() + ": " + directories[j].toString()
                            + ": Wrong number of files in the table");
                }
                for (short g = 0; g < files.length; ++g) {
                    if (files[g].isDirectory() || !fileNames.containsKey(files[g].getName())) {
                        throw new IOException(files[g].toString() + " is not a file of Date Base table");
                    }
                    idFile[1] = fileNames.get(files[g].getName());
                    FileMap currentFileMap = new FileMap(files[g].getCanonicalFile(), idFile);
                    currentFileMap.readerFile();
                    currentFileMap.setAside();
                }
                File[] checkOnEmpty = directories[j].listFiles();
                if (checkOnEmpty.length == 0) {
                    if (!directories[j].delete()) {
                        throw new Exception(directories[j] + ": Deleting error");
                    }
                }
            }
        }
    }

    public void create(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("create: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(command[1]).toString();
        File creatingTableFile = new File(correctName);
        if (creatingTableFile.exists()) {
            System.out.println(command[1] + " exists");
        } else {
            TableDate creatingTable = new TableDate(creatingTableFile);
            if (!bidDateBase.containsKey(command[1])) {
                bidDateBase.put(command[1], creatingTable);
            }
        }
    }

    public void drop(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("drop: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(command[1]).toString();
        File creatingTableFile = new File(correctName);
        if (!creatingTableFile.exists()) {
             System.out.println(command[1] + " not exists");
        } else {
            String[] argShell = new String[] {
                    "rm",
                    creatingTableFile.toPath().toString()
            };
            Shell.main(argShell);
            System.out.println("dropped");
            if (currentTable == bidDateBase.get(command[1])) {
                currentTable = null;
            }
            bidDateBase.remove(command[1]);
        }
    }

    public void use(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("drop: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(command[1]).toString();
        File creatingTableFile = new File(correctName);
        if (!creatingTableFile.exists()) {
            System.out.println(creatingTableFile.getName() + " not exists");
            currentTable = null;
        } else {
            TableDate creatingTable = new TableDate(creatingTableFile);
            bidDateBase.put(command[1], creatingTable);
            currentTable = bidDateBase.get(command[1]);
            System.out.println("using " + command[1]);
        }
    }

    public static void put(String[] command) throws Exception {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IOException("put: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+", 2);
        if (arg.length != 2) {
            throw new IOException("put: Wrong number of arguments");
        }
        currentTable.put(command);
    }

    public static void remove(String[] command) throws Exception {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IOException("remove: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IOException("remove: Wrong number of arguments");
        }
        currentTable.remove(command);
    }

    public static void get(String[] command) throws Exception {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IOException("get: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IOException("get: Wrong number of arguments");
        }
        currentTable.get(command);
    }

    public static void exit(String[] arg) {
         System.exit(0);
    }

}






