package simpledb;import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */


public class Catalog {



    private Map<Integer,Table> tables;  //这个是逻辑结构和物理结构的结合点 tableId - Table.DbFile

    class Table{
        int tableId;
        String tableName;
        DbFile dbFile;
        String pkeyField;
    }

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        tables=new HashMap<>();
        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        //check
        for (Map.Entry<Integer, Table> entry : tables.entrySet()) {
            if(entry.getValue().tableName.equals(name)){
                // todo 在测试bigOrderJoinsTest 的时候会产生表名冲突
               // throw new UnsupportedOperationException("目前不支持添加相同名字的table");
            }

        }
        Table table=new Table();
        table.dbFile=file;
        table.tableName=name;
        table.pkeyField=pkeyField;
        table.tableId=file.getId();
       tables.put(file.getId(),table);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here

        for (Map.Entry<Integer, Table> entry : tables.entrySet()) {
            if(entry.getValue().tableName.equals(name)){
                return entry.getKey();
            }

        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        return  getDbFile(tableid).getTupleDesc();

    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if(!isIdValid(tableid)){
            throw new IllegalArgumentException("id不合法");
        }
        return tables.get(tableid).dbFile;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if(!isIdValid(tableid)){
            throw new IllegalArgumentException("id不合法");
        }
        return tables.get(tableid).pkeyField;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return tables.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        if(!isIdValid(id)){
            throw new IllegalArgumentException("id不合法");
        }
        return tables.get(id).tableName;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        tables.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * 从disk上面结构化的file解析到内存 生成   tableName  field type,field type   解析成  TupleDesc  的TDItem 元数据item {@link TupleDesc}
     * 接着再addTable存到tables内存中表示当前存在的表
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        // TODO: 18-5-18 我加了这句话，因为如果用户给的catalog是相对路径，将会出错，所以这里先转换成绝对路径
        // TODO: 18-5-18  另外要注意的是表格的.dat文件必须和catalog同一个文件夹下才不会出错
        String absolutePath=new File(catalogFile).getAbsolutePath();
        String baseFolder = new File(absolutePath).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
    private boolean isIdValid(int id) {
        return tables.containsKey(id);
    }


}

