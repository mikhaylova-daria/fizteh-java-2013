package ru.fizteh.fivt.students.mikhaylova_daria.db;

import java.io.File;

public class DirDateBase {

    private File dir;

    private Short id;

    FileMap[] fileArray = new FileMap[16];

    DirDateBase() {

    }

    DirDateBase(File directory, Short id) {
        dir = directory;
        this.id = id;
        Short[] idFile = new Short[2];
        idFile[0] = this.id;
        for (short i = 0; i < 16; ++i) {
            File file = new File(directory.toPath().resolve(i + ".dat").toString());
            idFile[1] = i;
            fileArray[i] = new FileMap(file, idFile);
        }
    }

    void startWorking() {
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                System.err.println(dir.toPath().toString() + ": Creating directory error");
                System.exit(1);
            }
        }
    }

    void deleteEmptyDir() {
        File[] f = dir.listFiles();
        if (f != null) {
            if (f.length == 0) {
                if (!dir.delete()) {
                    System.err.println("Deleting directory error");
                    System.exit(1);
                }
            }
        } else {
            System.err.println("Internal error"); //не должна вылетать, потому что директория подаётся 100% существующая
        }
    }

}
