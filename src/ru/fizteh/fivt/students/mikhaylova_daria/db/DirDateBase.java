package ru.fizteh.fivt.students.mikhaylova_daria.db;

import java.io.File;

public class DirDateBase {

    private File dir;

    private Short id;

    private boolean isReady = false;

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

    void startWorking() throws Exception {
        if (!isReady) {
            if (!dir.exists()) {
                if (!dir.mkdir()) {
                    throw new Exception("Creating directory error");
                }
            }
            isReady = true;
        }
    }

    void deleteEmptyDir() throws Exception {
        File[] f = dir.listFiles();
        if (f != null) {
            if (f.length == 0) {
                if (!dir.delete()) {
                    throw new Exception("Deleting directory error");
                }
            }
        } else {
            throw new Exception("Internal error");
        }
        isReady = false;
    }

    int countChanges() {
        int numberOfChanges = 0;
        for (int i = 0; i < 16; ++i) {
            numberOfChanges += fileArray[i].numberOfChangesCounter();
        }
        return numberOfChanges;
    }

    int size() {
        int numberOfKeys = 0;
        for (int i = 0; i < 16; ++i) {
            numberOfKeys += fileArray[i].size();
        }
        return numberOfKeys;
    }

    int commit() {
        int numberOfChanges = 0;

        for (int i = 0; i < 16; ++i) {
            int changesInFile = fileArray[i].numberOfChangesCounter();
            if (changesInFile != 0) {
                try {
                    startWorking();
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
                fileArray[i].commit();
                numberOfChanges += changesInFile;
            }
        }
        return numberOfChanges;
    }

    int rollback() {
        int numberOfChanges = 0;
        for (int i = 0; i < 16; ++i) {
            numberOfChanges += fileArray[i].rollback();
        }
        return numberOfChanges;
    }

}
