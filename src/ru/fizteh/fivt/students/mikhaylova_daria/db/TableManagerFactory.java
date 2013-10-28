package ru.fizteh.fivt.students.mikhaylova_daria.db;

import ru.fizteh.fivt.storage.strings.*;

public class TableManagerFactory implements TableProviderFactory {
    TableManagerFactory() {}
    public TableManager create(String dir) throws IllegalArgumentException {
        if (dir == null) {
            throw new IllegalArgumentException("Argument is null");
        }
        return new TableManager(dir);
    }

}
