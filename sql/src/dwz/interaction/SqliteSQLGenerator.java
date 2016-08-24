package dwz.interaction;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Update by internetroot on 2014-09-06.
 */
public class SqliteSQLGenerator {
	private static String databaseIP = "";//���ݿ��ip
	
    private static String dbBaseName = "db_strategy_";//���ݿ���(�ǵ��ж��Ƿ�Ϸ���)
    private static String targetServer = "";//Ŀ��������
    private static String targetDB = "db_strategy_";//��db��
	
	
    private static Connection conn = null;
    private static Statement sm = null;
    private static String insert = "INSERT INTO";//����sql
    private static String values = "VALUES";//values�ؼ���
    private static List<String> tableList = new ArrayList<String>();//ȫ�ִ�ű����б�
    private static List<String> insertList = new ArrayList<String>();//ȫ�ִ��insertsql�ļ�������
    private static String filePath = "D://insertSQL.sql";//����·�� �������ݵ��ļ�

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
        	connectSQL("com.mysql.jdbc.Driver", "jdbc:mysql://"+databaseIP+"/"+dbBaseName+"?useUnicode=true&characterEncoding=UTF-8", "XXXXXX", "XXXXX");//�������ݿ�
          
        	ResultSet rs=sm.executeQuery("select * from role where name='"+player.get(j)+"'");
            String userId="";
            String roleid="";
            //boolean isgetUR=false;//����Ѿ�����user����role�����ݾ�����Ϊtrue
            while(rs.next()){//while(result.next)����˼�ǽ�rsȫ�����ж�ȡ��ֻ��һ�� 
            	userId=rs.getString("userid");
            	//roleid=rs.getString("roleid");
            }
            rs.close();
            ResultSet rs1=sm.executeQuery("select * from role where userId='"+userId+"'");
            List<String> roleidList=new ArrayList<String>();
            while(rs1.next()){//���滹��ִ��executeQuery�����ģ���رյ�����һ���Ի�ȡ
            	roleidList.add(rs1.getString("roleid"));
            }
            for(int m=0;m<roleidList.size();m++){
            	listSQL = createSQL(listSQL,targetDB);//������ѯ���
            	roleid=roleidList.get(m);
            	for(int i=0;i<listSQL.size();i++){
                	if(i<=2&&m==0){//m����0ʱ��ŵ�user��role�����ǾͲ�����
                		listSQL.set(i, listSQL.get(i)+"'"+userId+"'");
                	}else{
                		listSQL.set(i, listSQL.get(i)+"'"+roleid+"'");
                	}
                }
            	executeSQL(conn, sm, listSQL, tableList,targetServer);//ִ��sql��ƴװ
            	listSQL =getselectSql();
            }
            
            
            
            sm.close();
            conn.close();    
            createFile("F://ftp_client//ftp//"+player.get(j)+"_insert.sql");//�����ļ�
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
//        executeSQL(conn, sm, listSQL, tableList,info.get(4));//ִ��sql��ƴװ
//        createFile();//�����ļ�
        return null;
    }
 
    /**
     * ��ȡuser������Ϣ
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
            String rec = null;//һ��
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
     * ƴװ��ѯ���
     *
     * @return ����select����
     */
    private static List<String> createSQL(List<String> selectSQL,String database) throws Exception {
        List<String> listSQL = new ArrayList<String>();
        

        int i;//�����ĵ�һ���ַ�λ��
        int k;//���������һ���ַ���λ��
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
                //��ȡ���в�ѯ���
                listSQL.add(rec.toString());
            }

         
        return listSQL;
    }

    /**
     * ����insertsql.txt����������
     */
    private static void createFile(String tofile) {
        File file = new File(tofile);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                System.out.println("�����ļ���ʧ�ܣ���");
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
     * �������ݿ� ����statement����
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
     * ִ��sql�����ز���sql
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
     * ��ȡ��������ֵ
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
     * ƴװinsertsql �ŵ�ȫ��list����
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
    	
    	System.out.println("����Ҫ���ҵ����ݿ�ip(XXXXXX)��");
    	databaseIP= br.readLine();
    	System.out.println("����Ҫ���ҵ������ţ�����Ҫ���ҵ�db_strategy_30003��30003����");
    	dbBaseName+=br.readLine();
    	System.out.println("����Ҫ�����Ŀ�������ţ�����30031��Ҳ����server��locate��ֵ����");
    	targetServer=br.readLine();
    	System.out.println("����Ҫ�����Ŀ��DB���׺������db_strategy_0001������0001����");
    	targetDB=targetDB+br.readLine();

    
        String file2 = "/config/export_sqlite_data_select.cfg";
        executeSelectSQLFile(file2, null);

    }
}
