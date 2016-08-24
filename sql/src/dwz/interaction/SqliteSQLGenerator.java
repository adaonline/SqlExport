package dwz.interaction;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Update by internetroot on 2014-09-06.
 */
public class SqliteSQLGenerator {
	private static String databaseIP = "";//数据库的ip
	
    private static String dbBaseName = "db_strategy_";//数据库名(记得判断是否合服过)
    private static String targetServer = "";//目标区服号
    private static String targetDB = "db_strategy_";//标db库
	
	
    private static Connection conn = null;
    private static Statement sm = null;
    private static String insert = "INSERT INTO";//插入sql
    private static String values = "VALUES";//values关键字
    private static List<String> tableList = new ArrayList<String>();//全局存放表名列表
    private static List<String> insertList = new ArrayList<String>();//全局存放insertsql文件的数据
    private static String filePath = "D://insertSQL.sql";//绝对路径 导出数据的文件

    public static String generateTableDataSQL(String sql, String[] params) {
        return null;
    }

    public static List<String> getselectSql(){
    	List<String> listSQL = new ArrayList<String>();
    	listSQL.add("select * from user where userId=");
    	listSQL.add("select * from magic_sword_creat where userid=");
    	listSQL.add("select * from role where userId=");
    	listSQL.add("select * from eheart where roleId=");
    	listSQL.add("select * from magicbook where roleId=");
    	listSQL.add("select * from monsterbook where playerId=");
    	listSQL.add("select * from hired where roleId=");
    	
    	return listSQL;
    }
    
    public static String executeSelectSQLFile(String file, String[] params) throws Exception {
        List<String> listSQL =getselectSql();
        
        List<String> player = getinfo("/config/player.cfg");
        
        for(int j=0;j<player.size();j++){
        	connectSQL("com.mysql.jdbc.Driver", "jdbc:mysql://"+databaseIP+"/"+dbBaseName+"?useUnicode=true&characterEncoding=UTF-8", "XXXXXX", "XXXXX");//连接数据库
          
        	ResultSet rs=sm.executeQuery("select * from role where name='"+player.get(j)+"'");
            String userId="";
            String roleid="";
            //boolean isgetUR=false;//如果已经导过user或者role的数据就设置为true
            while(rs.next()){//while(result.next)的意思是将rs全部进行读取；只有一行 
            	userId=rs.getString("userid");
            	//roleid=rs.getString("roleid");
            }
            rs.close();
            ResultSet rs1=sm.executeQuery("select * from role where userId='"+userId+"'");
            List<String> roleidList=new ArrayList<String>();
            while(rs1.next()){//后面还有执行executeQuery方法的，会关闭掉，先一次性获取
            	roleidList.add(rs1.getString("roleid"));
            }
            for(int m=0;m<roleidList.size();m++){
            	listSQL = createSQL(listSQL,targetDB);//创建查询语句
            	roleid=roleidList.get(m);
            	for(int i=0;i<listSQL.size();i++){
                	if(i<=2&&m==0){//m等于0时候才导user和role，不是就不用了
                		listSQL.set(i, listSQL.get(i)+"'"+userId+"'");
                	}else{
                		listSQL.set(i, listSQL.get(i)+"'"+roleid+"'");
                	}
                }
            	executeSQL(conn, sm, listSQL, tableList,targetServer);//执行sql并拼装
            	listSQL =getselectSql();
            }
            
            
            
            sm.close();
            conn.close();    
            createFile("F://ftp_client//ftp//"+player.get(j)+"_insert.sql");//创建文件
            tableList.clear();
            insertList.clear();
        }
        
//        ResultSet rs=sm.executeQuery("select * from user where username='"+player.get(0)+"'");
//        String userId="";
//        String roleid="";
//        while(rs.next()){
//        	userId=rs.getString("userId");
//        }
//        ResultSet rs1=sm.executeQuery("select * from role where userid='"+userId+"'");
//        while(rs1.next()){
//        	roleid=rs1.getString("roleid");
//        }
//        for(int i=0;i<listSQL.size();i++){
//        	if(i<=1){
//        		listSQL.set(i, listSQL.get(i)+"'"+userId+"'");
//        	}else{
//        		listSQL.set(i, listSQL.get(i)+"'"+roleid+"'");
//        	}
//        }
//        executeSQL(conn, sm, listSQL, tableList,info.get(4));//执行sql并拼装
//        createFile();//创建文件
        return null;
    }
 
    /**
     * 获取user配置信息
     *
     * @return 
     */
    private static List<String> getinfo(String file) throws Exception {
        List<String> listinfo = new ArrayList<String>();
        BufferedReader br = null;
        InputStreamReader fr = null;
        InputStream is = null;
        try {
            is = SqliteSQLGenerator.class.getResourceAsStream(file);
            fr = new InputStreamReader(is);
            br = new BufferedReader(fr);
            String rec = null;//一行
            while ((rec = br.readLine()) != null) {
            	listinfo.add(rec.toString());
            }
        } finally {
            if (br != null) {
                br.close();
            }
            if (fr != null) {
                fr.close();
            }
            if (is != null) {
                is.close();
            }
        }
        return listinfo;
    }
    /**
     * 拼装查询语句
     *
     * @return 返回select集合
     */
    private static List<String> createSQL(List<String> selectSQL,String database) throws Exception {
        List<String> listSQL = new ArrayList<String>();
        

        int i;//表名的第一个字符位置
        int k;//表名单最后一个字符的位置
        String tableName;

     
            for(String rec:selectSQL) {
                rec = rec.toLowerCase();
                i = rec.indexOf("from ", 1) + 5;
                k = rec.indexOf(" ", i);
                if (k == -1) {
                    k = rec.length();
                }
                ;
                tableName = rec.substring(i, k);
                tableList.add(database+"."+tableName);
                //获取所有查询语句
                listSQL.add(rec.toString());
            }

         
        return listSQL;
    }

    /**
     * 创建insertsql.txt并导出数据
     */
    private static void createFile(String tofile) {
        File file = new File(tofile);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                System.out.println("创建文件名失败！！");
                e.printStackTrace();
            }
        }
       
       // FileWriter fw = null;
        BufferedWriter bw = null;
        try {
        	 OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(file),"UTF-8");
            //fw = new FileWriter(file);
            bw = new BufferedWriter(write);
           
            if (insertList.size() > 0) {
                for (int i = 0; i < insertList.size(); i++) {
                    bw.append(insertList.get(i));
                    bw.append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
               
               // fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 连接数据库 创建statement对象
     *
     * @param driver
     * @param url
     * @param UserName
     * @param Password
     */
    public static void connectSQL(String driver, String url, String UserName, String Password) {
        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(url, UserName, Password);
            sm = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行sql并返回插入sql
     *
     * @param conn
     * @param sm
     * @param listSQL
     * @throws java.sql.SQLException
     */
    public static void executeSQL(Connection conn, Statement sm, List listSQL, List listTable,String serverChange) throws SQLException {
        List<String> insertSQL = new ArrayList<String>();
        ResultSet rs = null;
        try {
            rs = getColumnNameAndColumeValue(sm, listSQL, listTable, rs,serverChange);
        } catch (SQLException e) {
            e.printStackTrace();
        } 
    }

    /**
     * 获取列名和列值
     *
     * @param sm
     * @param listSQL
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    private static ResultSet getColumnNameAndColumeValue(Statement sm,
                                                         List listSQL, List ListTable, ResultSet rs,String serverChange) throws SQLException {
        for (int j = 0; j < listSQL.size(); j++) {
            String sql = String.valueOf(listSQL.get(j));
            rs = sm.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                StringBuffer ColumnName = new StringBuffer();
                StringBuffer ColumnValue = new StringBuffer();
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    if (i == columnCount) {
                        ColumnName.append(rsmd.getColumnName(i));
                        if (Types.CHAR == rsmd.getColumnType(i) || Types.VARCHAR == rsmd.getColumnType(i)
                                || Types.LONGVARCHAR == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append("'").append(value).append("'");
                            }
                        } else if (Types.SMALLINT == rsmd.getColumnType(i) || Types.INTEGER == rsmd.getColumnType(i)
                                || Types.BIGINT == rsmd.getColumnType(i) || Types.FLOAT == rsmd.getColumnType(i)
                                || Types.DOUBLE == rsmd.getColumnType(i) || Types.NUMERIC == rsmd.getColumnType(i)
                                || Types.DECIMAL == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append("'").append(value).append("'");
                            }
                        } else if (Types.DATE == rsmd.getColumnType(i) || Types.TIME == rsmd.getColumnType(i)
                                || Types.TIMESTAMP == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append("'").append(value).append("'");
                            }
                        } else {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append("'").append(value).append("'");
                            }
                        }
                    } else {
                        ColumnName.append(rsmd.getColumnName(i)+ ",");
                        if (Types.CHAR == rsmd.getColumnType(i) || Types.VARCHAR == rsmd.getColumnType(i)
                                || Types.LONGVARCHAR == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append("'").append(value).append("',");
                            }
                        } else if (Types.SMALLINT == rsmd.getColumnType(i) || Types.INTEGER == rsmd.getColumnType(i)
                                || Types.BIGINT == rsmd.getColumnType(i) || Types.FLOAT == rsmd.getColumnType(i)
                                || Types.DOUBLE == rsmd.getColumnType(i) || Types.NUMERIC == rsmd.getColumnType(i)
                                || Types.DECIMAL == rsmd.getColumnType(i)) {
                           if(rsmd.getColumnName(i).equals("server")||rsmd.getColumnName(i).equals("locate")||rsmd.getColumnName(i).equals("country")||rsmd.getColumnName(i).equals("create_server")){
                        	   ColumnValue.append("'"+serverChange+"',");
                           }else{
                        	if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append("'").append(value).append("',");
                            }
                        	}
                        } else if (Types.DATE == rsmd.getColumnType(i) || Types.TIME == rsmd.getColumnType(i)
                                || Types.TIMESTAMP == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append("'").append(value).append("',");
                            }
                        } else {
                            if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append("'").append(value).append("',");
                            }
                        }
                    }
                }
                //System.out.println(ColumnName.toString());
                //System.out.println(ColumnValue.toString());
                insertSQL(ListTable.get(j).toString(), ColumnName, ColumnValue);
            }
        }
        return rs;
    }

    /**
     * 拼装insertsql 放到全局list里面
     *
     * @param ColumnName
     * @param ColumnValue
     */
    private static void insertSQL(String TableName, StringBuffer ColumnName,
                                  StringBuffer ColumnValue) {
        StringBuffer insertSQL = new StringBuffer();
        insertSQL.append(insert).append(" ").append(TableName).append("(").append(ColumnName.toString())
                .append(")").append(values).append("(").append(ColumnValue.toString()).append(");");
        insertList.add(insertSQL.toString());
        System.out.println(insertSQL.toString());
    }

    public static void main(String[] args) throws Exception {
        //String file1 = "/config/user.cfg";
        //executeSelectSQLFile(file1, null);
    	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    	
    	System.out.println("输入要查找的数据库ip(XXXXXX)：");
    	databaseIP= br.readLine();
    	System.out.println("输入要查找的区服号（例如要查找的db_strategy_30003的30003）：");
    	dbBaseName+=br.readLine();
    	System.out.println("输入要导入的目标区服号（例如30031，也就是server，locate的值）：");
    	targetServer=br.readLine();
    	System.out.println("输入要导入的目标DB库后缀（例如db_strategy_0001就输入0001）：");
    	targetDB=targetDB+br.readLine();

    
        String file2 = "/config/export_sqlite_data_select.cfg";
        executeSelectSQLFile(file2, null);

    }
}
