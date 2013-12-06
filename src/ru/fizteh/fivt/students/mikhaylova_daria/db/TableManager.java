package ru.fizteh.fivt.students.mikhaylova_daria.db;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import ru.fizteh.fivt.students.mikhaylova_daria.shell.Shell;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.DataFormatException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;


import ru.fizteh.fivt.storage.structured.*;

public class TableManager implements TableProvider, AutoCloseable {
    ConcurrentHashMap<String, TableData> bidDataBase = new ConcurrentHashMap<>();
    private boolean isClosed = false;
    private File mainDir;
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    private ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock(true);
    private Lock closeReadLock = closeLock.readLock();
    private Lock closeWriteLock = closeLock.writeLock();
    private final Lock myWriteLock = readWriteLock.writeLock();

    TableManager(String nameMainDir) throws IllegalArgumentException, IOException {
        if (nameMainDir == null) {
            throw new IllegalArgumentException("wrong type (Name of directory is null)");
        }
        if (nameMainDir.isEmpty()) {
            throw new IllegalArgumentException("wrong type (Name of directory is empty)");
        }
        mainDir = new File(nameMainDir);
        myWriteLock.lock();
        try {
            if (!mainDir.exists()) {
                if (!mainDir.mkdirs()) {
                    throw new IOException("wrong type (Creating " + nameMainDir + " is impossible)");
                }
            }
            if (!mainDir.isDirectory()) {
                throw new IllegalArgumentException("wrong type (" + nameMainDir + " is not a directory)");
            }
            try {
                cleaner();
            } catch (Exception e) {
                throw new IOException("wrong type (" + e.getMessage() + ")", e);
            }
        } finally {
            myWriteLock.unlock();
        }
    }

    private void cleaner() throws IOException {
        HashMap<String, Short> fileNames = new HashMap<String, Short>();
        HashMap<String, Short> dirNames = new HashMap<String, Short>();
        for (short i = 0; i < 16; ++i) {
            fileNames.put(i + ".dat", i);
            dirNames.put(i + ".dir", i);
        }
        File[] tables = mainDir.listFiles();
        File sign = new File(mainDir, "signature.tsv");
        if (tables == null) {
            throw new IOException("Unknown error");
        }
        for (short i = 0; i < tables.length; ++i) {
            if (tables[i].isFile()) {
                if (!tables[i].toPath().equals(sign.toPath())) {
                    throw new IllegalStateException(tables[i].toString() + " is not table");
                }
            }
            File[] directories = tables[i].listFiles();
            if (directories == null) {
                throw new IOException("Unknown error");
            }
            if (directories.length > 17) {
                throw new IllegalStateException(tables[i].toString() + ": Wrong number of files in the table");
            }
            Short[] idFile = new Short[2];
            for (short j = 0; j < directories.length; ++j) {
                sign = new File(tables[i], "signature.tsv");
                if (!directories[j].equals(sign)) {
                    if (directories[j].isFile() || !dirNames.containsKey(directories[j].getName())) {
                        throw new IllegalStateException(directories[j].toString() + " is not directory of table");
                    }
                    idFile[0] = dirNames.get(directories[j].getName());
                    File[] files = directories[j].listFiles();
                    if (files == null) {
                        throw new IOException("Unknown error");
                    }
                    if (files.length > 16) {
                        throw new IllegalStateException(tables[i].toString() + ": " + directories[j].toString()
                                + ": Wrong number of files in the table");
                    }
                    for (short g = 0; g < files.length; ++g) {
                        if (files[g].isDirectory() || !fileNames.containsKey(files[g].getName())) {
                            throw new IllegalStateException(files[g].toString() + " is not a file of Date Base table");
                        }
                        if (files[g].length() == 0) {
                            throw new IllegalArgumentException("Empty file " + files[g].toString());
                        }
                        idFile[1] = fileNames.get(files[g].getName());
                        FileMap currentFileMap = new FileMap(files[g].getCanonicalFile(), idFile);
                        try {
                            TableData tableDat = new TableData(tables[i], this);
                            currentFileMap.readFile(tableDat);
                        } catch (DataFormatException e) {
                            throw new IllegalArgumentException(e.getMessage(), e);
                        } catch (ParseException e) {
                            throw new IllegalArgumentException(e.getMessage(), e);
                        }
                    }
                    File[] checkOnEmpty = directories[j].listFiles();
                    if (checkOnEmpty != null) {
                        if (checkOnEmpty.length == 0) {
                            throw new IllegalArgumentException("Empty directory: " + directories[j].toString());
                        }
                    }
                }
            }
        }
    }

    public TableData createTable(String nameTable, List<Class<?>> columnTypes) throws IOException {
        closeReadLock.lock();
        try {
            if (isClosed) {
                throw new IllegalStateException("This provider is closed");
            }
            if (nameTable == null) {
                throw new IllegalArgumentException("wrong type (nameTable is null)");
            }
            nameTable = nameTable.trim();
            if (nameTable.isEmpty()) {
                throw new IllegalArgumentException("wrong type (nameTable is empty)");
            }
            if (nameTable.contains("\\") || nameTable.contains("/")
                    || nameTable.contains("\n") || nameTable.contains(".")) {
                throw new IllegalArgumentException("wrong type (Name of directory contains wrong characters)");
            }
            if (columnTypes == null) {
                throw new IllegalArgumentException("wrong type (list of types is null)");
            }
            if (columnTypes.isEmpty()) {
                throw new IllegalArgumentException("wrong type (list of types is empty)");
            }
            String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(nameTable).toString();
            File creatingTableFile = new File(correctName);
            TableData creatingTable = null;
            myWriteLock.lock();
            try {
                if (!creatingTableFile.exists()) {
                    creatingTable = new TableData(creatingTableFile, columnTypes, this);
                    if (!creatingTableFile.isDirectory()) {
                        throw new IllegalArgumentException("wrong type (" + correctName + "is not directory)");
                    }
                    if (!bidDataBase.containsKey(nameTable)) {
                        bidDataBase.put(nameTable, creatingTable);
                    }
                }
            } finally {
                myWriteLock.unlock();
            }
            return creatingTable;
        } finally {
            closeReadLock.unlock();
        }
    }

    public TableData getTable(String nameTable) throws IllegalArgumentException {
        closeReadLock.lock();
        try {

            if (isClosed) {
                throw new IllegalStateException("This provider is closed");
            }
            if (nameTable == null) {
                throw new IllegalArgumentException();
            }
            if (nameTable.isEmpty()) {
                throw new IllegalArgumentException("wrong type (nameTable is empty)");
            }
            if (nameTable.contains("\\") || nameTable.contains("/")
                    || nameTable.contains("\n") || nameTable.contains(".")) {
                throw new IllegalArgumentException("wrong type (Name of directory contains wrong characters)");
            }
            TableData table = null;
            String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(nameTable).toString();
            File creatingTableFile = new File(correctName);
            myWriteLock.lock();
            try {
                if (!creatingTableFile.exists()) {
                    if (bidDataBase.containsKey(nameTable)) {
                        bidDataBase.remove(nameTable);
                    }
                    return null;
                }
                if (bidDataBase.containsKey(nameTable)) {
                    table = bidDataBase.get(nameTable);
                } else {
                    if (creatingTableFile.exists()) {
                        try {
                            table = new TableData(creatingTableFile, this);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException("wrong type (" + e.getMessage() + ")", e);
                        } catch (IOException e) {
                            throw new IllegalArgumentException("wrong type (" + e.getMessage() + ")", e);
                        }
                        if (!bidDataBase.containsKey(nameTable)) {
                            bidDataBase.put(nameTable, table);
                        }
                    }
                }
            } finally {
                myWriteLock.unlock();
            }
            return table;
        } finally {
            closeReadLock.unlock();
        }
    }

    public void removeTable(String nameTable) throws IOException {
        closeReadLock.lock();
        try {
            if (isClosed) {
                throw new IllegalStateException("This provider is closed");
            }
            if (nameTable == null) {
                throw new IllegalArgumentException("wrong type (nameTable is null)");
            }
            nameTable = nameTable.trim();
            if (nameTable.isEmpty()) {
                throw new IllegalArgumentException("wrong type (nameTable is empty)");
            }
            if (nameTable.contains("\\") || nameTable.contains("/")
                    || nameTable.contains("\n") || nameTable.contains(".")) {
                throw new IllegalArgumentException("wrong type (Name of directory contains wrong characters)");
            }
            String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(nameTable).toString();
            File creatingTableFile = new File(correctName);
            myWriteLock.lock();
            try {
                if (!creatingTableFile.exists()) {
                    throw new IllegalStateException("wrong type (Table " + nameTable + "does not exist)");
                } else {
                    String[] argShell = new String[] {
                            "rm",
                            creatingTableFile.toPath().toString()
                    };
                    Shell.main(argShell);
                    bidDataBase.remove(nameTable);
                }
            } finally {
                myWriteLock.unlock();
            }
        } finally {
            closeReadLock.unlock();
        }
    }

    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        closeReadLock.lock();
        try {
            if (isClosed) {
                throw new IllegalStateException("This provider is closed");
            }
            if (table == null) {
                throw new IllegalArgumentException("wrong type (table is null)");
            }
            if (value == null) {
                throw new IllegalArgumentException("wrong type (value is null)");
            }
            try {       //проверяем, что в передаваемом storeable столбцов не больше, чем в сигнатуре таблицы
                value.getColumnAt(table.getColumnsCount());
                throw new ColumnFormatException("Wrong number of columns in value");
            } catch (IndexOutOfBoundsException e) {
            }
            int i = 0;
            String xmlString;
            try {
                String valueStr = null;
                DocumentBuilderFactory factoryDoc = DocumentBuilderFactory.newInstance();
                DocumentBuilder docBuilder = factoryDoc.newDocumentBuilder();
                Document doc = docBuilder.newDocument();
                Element row = doc.createElement("row");
                doc.appendChild(row);
                for (i = 0; i < table.getColumnsCount(); ++i) {
                    if (!(value.getColumnAt(i) == null)) {
                        Element column = doc.createElement("col");
                        row.appendChild(column);
                        if (table.getColumnType(i).equals(Integer.class)) {
                            valueStr = value.getIntAt(i).toString();
                        }
                        if (table.getColumnType(i).equals(Long.class)) {
                            valueStr = value.getLongAt(i).toString();
                        }
                        if (table.getColumnType(i).equals(Byte.class)) {
                            valueStr = value.getByteAt(i).toString();
                        }
                        if (table.getColumnType(i).equals(Float.class)) {
                            valueStr = value.getFloatAt(i).toString();
                        }
                        if (table.getColumnType(i).equals(Double.class)) {
                            valueStr = value.getDoubleAt(i).toString();
                        }
                        if (table.getColumnType(i).equals(Boolean.class)) {
                            valueStr = value.getBooleanAt(i).toString();
                        }
                        if (table.getColumnType(i).equals(String.class)) {
                            valueStr = value.getStringAt(i);
                        }
                        Text text = doc.createTextNode(valueStr);
                        column.appendChild(text);
                    } else {
                        Element nil = doc.createElement("null");
                        row.appendChild(nil);
                    }
                }
                TransformerFactory transFac = TransformerFactory.newInstance();
                Transformer trans = transFac.newTransformer();
                trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                StringWriter sw = new StringWriter();
                StreamResult result = new StreamResult(sw);
                DOMSource source = new DOMSource(doc);
                trans.transform(source, result);
                xmlString = sw.toString();

            } catch (Exception numFormExc) {
                throw new ColumnFormatException("wrong type (Wrong type of argument " + i
                        + " or " + numFormExc.getMessage() + ")");
            }

            return xmlString;
        } finally {
            closeReadLock.unlock();
        }
    }

    public Storeable deserialize(Table table, String value) throws ParseException {
        closeReadLock.lock();
        try {
            if (isClosed) {
                throw new IllegalStateException("This provider is closed");
            }
            if (table == null) {
                throw new IllegalArgumentException("wrong type (table is null)");
            }
            if (value == null) {
                throw new IllegalArgumentException("wrong type (value is null)");
            }
            if (value.isEmpty()) {
                throw new IllegalArgumentException("wrong type (value is empty)");
            }
            int numberColumn = 0;
            Storeable storeableVal = new Value(table);
            try {
                DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder xmlDocBuilder = docFactory.newDocumentBuilder();
                ByteArrayInputStream theStream = new ByteArrayInputStream(value.getBytes());
                Document xmlDoc = xmlDocBuilder.parse(theStream);
                Element row = xmlDoc.getDocumentElement();
                if (!row.getTagName().equals("row")) {
                    throw new ParseException("wrong type (Bad first tag. \"row\" expected, but " + row.getTagName()
                            + "was)", 0);
                }
                NodeList children = row.getChildNodes();
                for (int i = 0; i < children.getLength(); i++) {
                    Node child  = children.item(i);
                    String valueColumnStr;
                    if (child.getNodeName().equals("col")) {
                        valueColumnStr = child.getTextContent();
                        ++numberColumn;
                    } else {
                        if (child.getNodeName().equals("null")) {
                            valueColumnStr = null;
                            ++numberColumn;
                        } else {
                            throw new ParseException("wrong type (Unknown tag)", i);
                        }
                    }
                    try {
                        if (valueColumnStr == null) {
                            storeableVal.setColumnAt(i, null);
                        } else {
                            if (table.getColumnType(i).equals(Integer.class)) {
                                Integer valueInt = Integer.parseInt(valueColumnStr.trim());
                                storeableVal.setColumnAt(i, valueInt);
                            }
                            if (table.getColumnType(i).equals(Long.class)) {
                                Long valueLong = Long.parseLong(valueColumnStr.trim());
                                storeableVal.setColumnAt(i, valueLong);
                            }
                            if (table.getColumnType(i).equals(Byte.class)) {
                                Byte valueByte = Byte.parseByte(valueColumnStr.trim());
                                storeableVal.setColumnAt(i, valueByte);
                            }
                            if (table.getColumnType(i).equals(Float.class)) {
                                Float valueFloat = Float.parseFloat(valueColumnStr.trim());
                                storeableVal.setColumnAt(i, valueFloat);
                            }
                            if (table.getColumnType(i).equals(Double.class)) {
                                Double valueDouble = Double.parseDouble(valueColumnStr.trim());
                                storeableVal.setColumnAt(i, valueDouble);
                            }
                            if (table.getColumnType(i).equals(Boolean.class)) {
                                Boolean valueBoolean = Boolean.parseBoolean(valueColumnStr.trim());
                                storeableVal.setColumnAt(i, valueBoolean);
                            }
                            if (table.getColumnType(i).equals(String.class)) {
                                storeableVal.setColumnAt(i, valueColumnStr.trim());
                            }
                        }
                    } catch (NumberFormatException e) {
                        throw new ColumnFormatException("wrong type (Wrong type of column " + i
                                + " " + table.getColumnType(i).getCanonicalName() + " was expected)");
                    }
                }
            } catch (SAXException e) {
                throw new ParseException(e.getMessage(), 0);
            } catch (ParserConfigurationException e) {
                throw new ParseException(e.getMessage(), 0);
            } catch (IOException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
            if (numberColumn != table.getColumnsCount()) {
                throw new ParseException("wrong type (Wrong number of columns:" + numberColumn + "but expected "
                        + table.getColumnsCount() + ")", 0);
            }
            return storeableVal;
        } finally {
            closeReadLock.unlock();
        }
    }

    public Storeable  createFor(Table table) {
        closeReadLock.lock();
        try {
            if (isClosed) {
                throw new IllegalStateException("This provider is closed");
            }
            if (table == null) {
                throw new IllegalArgumentException("wrong type (table is null)");
            }
            return new Value(table);
        } finally {
            closeReadLock.unlock();
        }
    }


    public Storeable createFor(Table table, List<?> values) throws ColumnFormatException, IndexOutOfBoundsException {
        closeReadLock.lock();
        try {
            if (isClosed) {
                throw new IllegalStateException("This provider is closed");
            }
            if (table == null) {
                throw new IllegalArgumentException("wrong type (table is null)");
            }
            if (values == null) {
                throw new IllegalArgumentException("wrong type (values list is null)");
            }
            if (values.isEmpty()) {
                throw new IllegalArgumentException("wrong type (values list is empty)");
            }
            if (table.getColumnsCount() != values.size()) {
                throw new IndexOutOfBoundsException("wrong type (Different size of list values and types)");
            }
            Storeable created = new Value(table);
            try {
                for (int i = 0; i < table.getColumnsCount(); ++i) {
                    created.setColumnAt(i, values.get(i));
                }
            } catch (ColumnFormatException e) {
                throw new ColumnFormatException("wrong type (" + e.getMessage() + ")", e);
            }  catch (IndexOutOfBoundsException e) {
                throw new IndexOutOfBoundsException("wrong type (" + e.getMessage() + ")");
            }
            return created;
        } finally {
            closeReadLock.unlock();
        }
    }

    public void close() {
        if (!isClosed) {
            closeWriteLock.lock();
            try {
                isClosed = true;
                for (TableData table: bidDataBase.values()) {
                    table.close();
                }
                bidDataBase.clear();
            } finally {
                closeWriteLock.unlock();
            }
        }
    }

    public String toString() {
        closeReadLock.lock();
        try {
            if (isClosed) {
                throw new IllegalStateException("table is closed");
            }
            return this.getClass().getSimpleName() + "[" + mainDir.toPath().toAbsolutePath() + "]";
        } finally {
            closeReadLock.unlock();
        }
    }
}
