package ru.fizteh.fivt.students.mikhaylova_daria.db;

import ru.fizteh.fivt.storage.strings.*;

public class Tester {

     public static void main(String[] arg) {
         TableProviderFactory factory = new TableManagerFactory();
         TableProvider manager = factory.create("/home/darya/db");
         Table table1 = manager.getTable("Дарья");
         table1.put("1", "2");
         System.out.println(table1.get("1"));
         System.out.println(table1.commit());
//         Table table2 = manager.getTable("ghj");
//         table2.remove("key");
//         table2.commit();
//         manager.removeTable("ghj");

    }
}
