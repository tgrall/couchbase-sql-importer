package com.couchbase.util;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;
import com.google.gson.Gson;

import java.io.FileInputStream;
import java.net.URI;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SqlImporter {


    private static final String COUCHBASE_URIS = "cb.uris";
    private static final String COUCHBASE_BUCKET = "cb.bucket";
    private static final String COUCHBASE_PASSWORD = "cb.password";

    private static final String SQL_CONN_STRING = "sql.connection";
    private static final String SQL_USER = "sql.username";
    private static final String SQL_PASSWORD = "sql.password";

    private static final String TABLES_LIST = "import.tables";
    private static final String CREATE_VIEWS = "import.createViews";
    private static final String TYPE_FIELD = "import.typefield";
    private static final String TYPE_CASE = "import.fieldcase";

    private static final String ID_FIELD = "generate.idField";
    private static final String DATE_FIELD = "generate.dateField";
    private static final String YEAR_FIELD = "generate.yearField";
    private static final String MONTH_FIELD = "generate.monthField";
    private static final String DAY_FIELD = "generate.dayField";
    private static final String HOUR_FIELD = "generate.hourField";
    private static final String REPEAT_COUNT_FIELD = "generate.repeatCount";
    private static final String REPEAT_STEP_FIELD = "generate.repeatStepMillis";

    private CouchbaseClient couchbaseClient = null;
    private Connection connection = null;

    // JDBC connection
    private String sqlConnString = null;
    private String sqlUser = null;
    private String sqlPassword = null;

    // Couchbase information
    private List uris = new ArrayList();
    private String bucket = "default";
    private String defaultUri = "http://127.0.0.1:8091/pools";
    private String password = "";

    // import options
    private String typeField = null;
    private String typeFieldCase = null;
    private boolean createTableViewEnable = true;
    private String[] tableList = null;

    private String idField = null;
    private String dateField = null;
    private String yearField = null;
    private String monthField = null;
    private String dayField = null;
    private String hourField = null;
    private int repeatCount = -1;
    private long repeatStepMillis = -1;


    public static void main(String[] args) {


        System.out.println("\n\n");
        System.out.println("############################################");
        System.out.println("#         COUCHBASE SQL IMPORTER           #");
        System.out.println("############################################\n\n");


        if (args == null || args.length == 0) {
            System.out.println("ERROR : You must specify a propeprty file when you run this command\n\n");
            System.exit(0);
        }

        SqlImporter importer = new SqlImporter();
        try {


            // remove log info from Couchbase
            Properties systemProperties = System.getProperties();
            systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
            System.setProperties(systemProperties);
            Logger logger = Logger.getLogger("com.couchbase.client");
            logger.setLevel(Level.WARNING);
            for (Handler h : logger.getParent().getHandlers()) {
                if (h instanceof ConsoleHandler) {
                    h.setLevel(Level.WARNING);
                }
            }


            importer.setup(args[0]);
            importer.importData();
            importer.shutdown();


        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("\n\n              FINISHED");
        System.out.println("############################################");
        System.out.println("\n\n");


    }

    private void setup(String fileName) {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream(fileName));

            if (prop.containsKey(COUCHBASE_URIS)) {
                String[] uriStrings = prop.getProperty(COUCHBASE_URIS).split(",");
                for (int i = 0; i < uriStrings.length; i++) {
                    uris.add(new URI(uriStrings[i]));
                }

            } else {
                uris.add(new URI("http://127.0.0.1:8091/pools"));
            }

            if (prop.containsKey(COUCHBASE_BUCKET)) {
                bucket = prop.getProperty(COUCHBASE_BUCKET);
            }

            if (prop.containsKey(COUCHBASE_PASSWORD)) {
                password = prop.getProperty(COUCHBASE_PASSWORD);
            }

            if (prop.containsKey(SQL_CONN_STRING)) {
                sqlConnString = prop.getProperty(SQL_CONN_STRING);
            } else {
                throw new Exception(" JDBC Connection String not specified");
            }

            if (prop.containsKey(SQL_USER)) {
                sqlUser = prop.getProperty(SQL_USER);
            } else {
                throw new Exception(" JDBC User not specified");
            }

            if (prop.containsKey(SQL_PASSWORD)) {
                sqlPassword = prop.getProperty(SQL_PASSWORD);
            } else {
                throw new Exception(" JDBC Password not specified");
            }

            if (prop.containsKey(TABLES_LIST)) {
                tableList = prop.getProperty(TABLES_LIST).split(",");
            }

            if (prop.containsKey(CREATE_VIEWS)) {
                createTableViewEnable = Boolean.parseBoolean(prop.getProperty(CREATE_VIEWS));
            }

            if (prop.containsKey(TYPE_FIELD)) {
                typeField = prop.getProperty(TYPE_FIELD);
            }

            if (prop.containsKey(TYPE_CASE)) {
                typeFieldCase = prop.getProperty(TYPE_CASE);
            }

            if (prop.containsKey(ID_FIELD)) {
                idField = prop.getProperty(ID_FIELD);
            }

            if (prop.containsKey(DATE_FIELD)) {
                dateField = prop.getProperty(DATE_FIELD);
            }

            if (prop.containsKey(YEAR_FIELD)) {
                yearField = prop.getProperty(YEAR_FIELD);
            }

            if (prop.containsKey(DAY_FIELD)) {
                dayField = prop.getProperty(DAY_FIELD);
            }

            if (prop.containsKey(MONTH_FIELD)) {
                monthField = prop.getProperty(MONTH_FIELD);
            }

            if (prop.containsKey(HOUR_FIELD)) {
                hourField = prop.getProperty(HOUR_FIELD);
            }

            if (prop.containsKey(REPEAT_COUNT_FIELD)) {
                repeatCount = Integer.parseInt(prop.getProperty(REPEAT_COUNT_FIELD));
            }

            if (prop.containsKey(REPEAT_STEP_FIELD)) {
                repeatStepMillis = Long.parseLong(prop.getProperty(REPEAT_STEP_FIELD));
            }

            System.out.println("\nImporting table(s)");
            System.out.println("\tfrom : \t" + sqlConnString);
            System.out.println("\tto : \t" + uris + " - " + bucket);


        } catch (Exception e) {
            System.out.println(e.getMessage() + "\n\n");
            System.exit(0);
        }

    }

    private void shutdown() throws SQLException {
        if (couchbaseClient != null) {
            couchbaseClient.shutdown(5, TimeUnit.SECONDS);
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }

    }


    public void SqlImporter() {
    }


    public void importData() throws Exception {
        if (tableList == null || tableList[0].equalsIgnoreCase("ALL")) {
            this.importAllTables();
        } else {
            for (int i = 0; i < tableList.length; i++) {
                this.importTable(tableList[i].trim());
            }
        }

        if (createTableViewEnable) {
            createTableViews();
        }

    }


    public void importAllTables() throws Exception {
        DatabaseMetaData md = this.getConnection().getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
        while (rs.next()) {
            String tableName = rs.getString(3);
            importTable(tableName);
        }
    }

    public void importTable(String tableName) throws Exception {
        System.out.println("\n  Exporting Table : " + tableName);
        String typeName = this.getNamewithCase(tableName, typeFieldCase);
        if (createTableViewEnable) {
            this.createViewsForPrimaryKey(tableName);
        }
        PreparedStatement preparedStatement = null;
        String selectSQL = "SELECT * FROM " + tableName;
        Gson gson = new Gson();
        try {

            preparedStatement = this.getConnection().prepareStatement(selectSQL);
            ResultSet rs = preparedStatement.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int numColumns = rsmd.getColumnCount();

            int rowCount = 0;
            int rowsMoved = 0;
            while (rs.next()) {
                rowCount++;
                Map<String, Object> map = new HashMap<String, Object>();

                for (int i = 1; i < numColumns + 1; i++) {
                    String columnName = this.getNamewithCase(rsmd.getColumnName(i), typeFieldCase);

                    if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
                        map.put(columnName, rs.getArray(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
                        map.put(columnName, rs.getLong(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
                        map.put(columnName, rs.getBoolean(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
                        map.put(columnName, rs.getBlob(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
                        map.put(columnName, rs.getDouble(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
                        map.put(columnName, rs.getFloat(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
                        map.put(columnName, rs.getInt(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
                        map.put(columnName, rs.getNString(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
                        map.put(columnName, rs.getString(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
                        map.put(columnName, rs.getInt(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
                        map.put(columnName, rs.getInt(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
                        map.put(columnName, rs.getDate(columnName));
                    } else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
                        map.put(columnName, rs.getTimestamp(columnName));
                    } else {
                        map.put(columnName, rs.getObject(columnName));
                    }
                }

                if (typeField != null && typeField.length() > 0) {
                    map.put(typeField, typeName);
                }

                String originalId = (idField != null && idField.length() > 0) ?
                        map.get(idField).toString() :
                        tableName + ":" + Integer.toString(rowCount);
                if (dateField != null && dateField.length() > 0) {
                    String dateString = map.get(dateField).toString();
                    Calendar calendar = Calendar.getInstance();
                    SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM d, yyyy hh:mm:ss a");    // "Jan 14, 2016 10:06:35 PM"
                    SimpleDateFormat backupFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");    // "2016-03-15 19:04:29.0"
                    Date startDate = dateFormatter.parse(dateString, new ParsePosition(0));
                    if (startDate == null) {
                        startDate = backupFormatter.parse(dateString, new ParsePosition(0));
                    }
                    if (startDate == null) {
                        System.out.println("Skipped " + dateString);
                        continue;
                    }

                    System.out.println("Writing " + originalId + " x" + Integer.toString(repeatCount));
                    long t = startDate.getTime();
                    for (int i = 0; i < repeatCount; i++) {
                        t += repeatStepMillis;
                        Date d = new Date(t);
                        map.put(dateField, dateFormatter.format(d));
                        if (yearField != null && yearField.length() > 0) {
                            map.put(yearField, calendar.get(Calendar.YEAR));
                        }
                        if (monthField != null && monthField.length() > 0) {
                            map.put(monthField, calendar.get(Calendar.MONTH));
                        }
                        if (dayField != null && dayField.length() > 0) {
                            map.put(dayField, calendar.get(Calendar.DAY_OF_MONTH));
                        }
                        if (hourField != null && hourField.length() > 0) {
                            map.put(hourField, calendar.get(Calendar.HOUR_OF_DAY));
                        }
                        String key = originalId + "_" + Integer.toString(i);
                        this.getCouchbaseClient().set(key, gson.toJson(map)).get();
                    }
                } else {
                    this.getCouchbaseClient().set(originalId, gson.toJson(map)).get();
                }

                rowsMoved++;
            }
            System.out.println("    " + rowsMoved + " out of " + rowCount + " records moved to Couchbase.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    private void createViewsForPrimaryKey(String tableName) {
        String typeName = this.getNamewithCase(tableName, typeFieldCase);
        List<String> pkCols = new ArrayList();
        DatabaseMetaData databaseMetaData = null;
        try {
            databaseMetaData = this.getConnection().getMetaData();
            ResultSet rs = databaseMetaData.getPrimaryKeys(null, null, tableName);
            while (rs.next()) {
                pkCols.add(rs.getString(4));
            }
            String[] array = pkCols.toArray(new String[pkCols.size()]);

            StringBuilder mapFunction = new StringBuilder();
            StringBuilder ifStatement = new StringBuilder();
            StringBuilder emitStatement = new StringBuilder();


            mapFunction.append("function (doc, meta) {\n")
                       .append("  var idx = (meta.id).indexOf(\":\");\n")
                       .append("  var docType = (meta.id).substring(0,idx); \n");


            if (array != null && array.length == 1) {
                ifStatement.append("  if (meta.type == 'json' && docType == '")
                           .append(typeName)
                           .append("'  && doc.")
                           .append( this.getNamewithCase( array[0], typeFieldCase) )
                           .append(" ){ \n");
                emitStatement.append("    emit(doc.").append(array[0]).append(");");
            } else if (array != null && array.length > 1) {
                emitStatement.append("    emit([");
                ifStatement.append("  if (meta.type == 'json' && docType == '")
                           .append(typeName)
                           .append("'  && ");

                for (int i = 0; i < array.length; i++) {
                    emitStatement.append("doc.").append( this.getNamewithCase( array[i], typeFieldCase) );
                    ifStatement.append("doc.").append( this.getNamewithCase( array[i], typeFieldCase) );
                    if (i < (array.length-1)) {
                      emitStatement.append(", ");
                      ifStatement.append(" && ");
                    }
                }
                ifStatement.append(" ){\n");
                emitStatement.append("]);\n");
            }

            mapFunction.append( ifStatement )
                       .append(emitStatement)
                       .append("  }\n")
                       .append("}\n");


            System.out.println("\n\n Create Couchbase views for table "+ typeName);
            DesignDocument designDoc = new DesignDocument(tableName);
            String viewName = "by_pk";
            String reduceFunction = "_count";
            ViewDesign viewDesign = new ViewDesign(viewName, mapFunction.toString(), reduceFunction);
            designDoc.getViews().add(viewDesign);
            getCouchbaseClient().createDesignDoc(designDoc);



        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private void createTableViews() throws Exception {


        System.out.println("\n\n Create Couchbase views for 'types' ....");

        DesignDocument designDoc = new DesignDocument("all");

        String viewName = "by_type";
        String mapFunction =
                "function (doc, meta) {\n" +
                        "  if (meta.type == \"json\") {\n" +
                        "    var idx = (meta.id).indexOf(\":\");\n" +
                        "    emit( (meta.id).substring(0,idx));\n" +
                        "  }  \n" +
                        "}";
        String reduceFunction = "_count";
        ViewDesign viewDesign = new ViewDesign(viewName, mapFunction, reduceFunction);
        designDoc.getViews().add(viewDesign);
        getCouchbaseClient().createDesignDoc(designDoc);

    }


    public CouchbaseClient getCouchbaseClient() throws Exception {
        if (couchbaseClient == null) {
            couchbaseClient = new CouchbaseClient(uris, bucket, password);
        }
        return couchbaseClient;


    }

    public void setCouchbaseClient(CouchbaseClient couchbaseClient) {
        this.couchbaseClient = couchbaseClient;
    }

    public Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(sqlConnString, sqlUser, sqlPassword);
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private String getNamewithCase(String tablename, String nameType) {
        String returnValue =  tablename;
        if (nameType.equalsIgnoreCase("lower")) {
            returnValue = tablename.toLowerCase();
        } else if (nameType.equalsIgnoreCase("upper")) {
            returnValue = tablename.toUpperCase();
        }
        return returnValue;
    }
}
