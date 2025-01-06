package seeyou.db.notify;

import com.seeyou.logging.MyLogger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Septian Yudhistira
 * @version 1.0
 * @since 2025-01-06
 */
public class ListenerNotify implements Runnable{
    public static final BlockingQueue<String> pushQueryQueue = new ArrayBlockingQueue(1000);
    private Connection conn = null;
    private final String url;
    private final String user;
    private final String pass;
    private final String urlPost;
    private final String key;

    public ListenerNotify(String key, String url, String user, String pass) {
        this.key = key;
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.urlPost = "http://43.229.84.189:7092/v1/api/db/push";
        this.initNotify();
    }

    private void initNotify() {
        if (this.conn != null) {
            try {
                this.conn.close();
            } catch (Exception var3) {
            }
        }

        try {
            MyLogger.dbNotify("init new notify listener : " + InitDBTrigger.DB_NOTIFY_NAME);
            Class.forName("org.postgresql.Driver");
            this.conn = DriverManager.getConnection(this.url, this.user, this.pass);
            Statement stmt = this.conn.createStatement();
            stmt.execute("LISTEN " + InitDBTrigger.DB_NOTIFY_NAME);
            stmt.close();
            MyLogger.dbNotify("Success init notify listener : " + InitDBTrigger.DB_NOTIFY_NAME);
        } catch (Exception var2) {
            MyLogger.error(var2.getMessage(), var2);
        }

    }

    public void run() {
        try {
            Statement stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1");
            rs.close();
            stmt.close();
            PGNotification[] notifications = ((PGConnection)this.conn).getNotifications();
            if (notifications != null) {
                List<Map<String, String>> sqls = new ArrayList();
                PGNotification[] var5 = notifications;
                int var6 = notifications.length;

                for(int var7 = 0; var7 < var6; ++var7) {
                    PGNotification notification = var5[var7];
                    Map<String, String> sqlMap = new HashMap();
                    String json = notification.getParameter();

                    try {
                        JSONParser jsonParser = new JSONParser();
                        Map<String, Object> notifymap = (Map)jsonParser.parse(json);
                        String table = (String)notifymap.get("p_tbl");
                        long id = (Long)notifymap.get("p_id");
                        String stringId = String.valueOf(id);
                        String type = (String)notifymap.get("p_type");
                        String idType = stringId + type;
                        if (pushQueryQueue.contains(idType)) {
                            pushQueryQueue.remove(idType);
                            MyLogger.dbNotify("json ignore notify : " + json);
                        } else {
                            String sql = "";
                            if (type.equals("DELETE")) {
                                sql = "DELETE FROM " + table + " WHERE " + table + ".id = '" + id + "'";
                            } else if (type.equals("UPDATE")) {
                                String sqlParamUpdate = this.getSqlParamUpdate(this.getDataDB(table, id));
                                sql = "UPDATE " + table + " SET " + sqlParamUpdate + " WHERE " + table + ".id = '" + id + "'";
                            } else if (type.equals("INSERT")) {
                                sql = this.getInsertQuery(table, this.getDataDB(table, id));
                            }

                            sqlMap.put("id", stringId);
                            sqlMap.put("sql", sql);
                            sqlMap.put("type", type);
                            sqlMap.put("idtype", idType);
                            sqls.add(sqlMap);
                        }
                    } catch (Exception var21) {
                        MyLogger.error(json, var21);
                    }
                }

                if (!sqls.isEmpty()) {
                    Map<String, Object> messageMap = new HashMap();
                    messageMap.put("KEY", this.key);
                    messageMap.put("SQLS", sqls);
                    AtomicInteger retryConnection = new AtomicInteger(1);
                    String requestJson = (new JSONObject(messageMap)).toJSONString();
                    MyLogger.dbNotify("PUSH_DB_NOTIFY_REQUEST_" + retryConnection.get() + " : " + requestJson);
                    String responseJson = this.sendHttpPost(retryConnection, this.urlPost, 60000, requestJson);
                    MyLogger.dbNotify("PUSH_DB_NOTIFY_RESPONSE_" + retryConnection.get() + " : " + responseJson);
                }
            }
        } catch (Exception var22) {
            MyLogger.error(var22.getMessage(), var22);
            MyLogger.dbNotify("Reconnect notify listener : " + InitDBTrigger.DB_NOTIFY_NAME);
            this.initNotify();
        }

    }

    public String getInsertQuery(String tableName, Map<String, String> dataMap) {
        StringBuilder sql = new StringBuilder();

        try {
            sql.append("INSERT INTO ").append(tableName).append(" (");
            StringBuilder placeholders = new StringBuilder();
            Iterator iter = dataMap.keySet().iterator();

            while(iter.hasNext()) {
                String key = (String)iter.next();
                String val = (String)dataMap.get(key);
                if (val != null && !val.isEmpty()) {
                    if (!val.startsWith("(")) {
                        val = val.replace("'", "''");
                    }

                    sql.append(key);
                    placeholders.append(val.startsWith("(") ? "" : "'");
                    placeholders.append(val);
                    placeholders.append(val.endsWith(")") ? "" : "'");
                    if (iter.hasNext()) {
                        sql.append(", ");
                        placeholders.append(", ");
                    }
                }
            }

            sql.append(") VALUES (").append(placeholders).append(")");
        } catch (Exception var8) {
            MyLogger.error(var8.getMessage() + ", " + tableName + ", data map : " + dataMap, var8);
        }

        return sql.toString();
    }

    public String getSqlParamUpdate(Map<String, String> dataMap) {
        StringBuilder sql = new StringBuilder();
        Iterator iter = dataMap.keySet().iterator();

        while(iter.hasNext()) {
            String key = (String)iter.next();
            String val = (String)dataMap.get(key);
            if (val != null && !val.isEmpty()) {
                sql.append(key).append("=").append("'").append(val.replace("'", "''")).append("'");
                if (iter.hasNext()) {
                    sql.append(", ");
                }
            }
        }

        return sql.toString();
    }

    public Map<String, String> getDataDB(String table, long id) {
        Object resultMap = new HashMap();

        try {
            String sqlSelect = "SELECT * FROM " + table + " WHERE " + table + ".id = '" + id + "'";
            Statement statement = this.conn.createStatement();
            ResultSet rs1 = statement.executeQuery(sqlSelect);
            resultMap = (Map)this.getListMapResult(rs1).get(0);
            rs1.close();
            statement.close();
        } catch (Exception var8) {
            MyLogger.error(var8.getMessage(), var8);
        }

        return (Map)resultMap;
    }

    public List<Map<String, String>> getListMapResult(ResultSet rs) {
        ArrayList returnValue = new ArrayList();

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int size = metaData.getColumnCount();

            while(rs.next()) {
                Map<String, String> map = new HashMap();

                for(int i = 0; i < size; ++i) {
                    int index = i + 1;
                    String key = metaData.getColumnLabel(index);
                    String val = rs.getString(index);
                    map.put(key, val);
                }

                returnValue.add(map);
            }
        } catch (Exception var10) {
            MyLogger.error(var10.getMessage(), var10);
        }

        return returnValue;
    }

    public String sendHttpPost(AtomicInteger ai, String httpAddress, int httpTimeout, String requestMessage) {
        StringBuffer sb = new StringBuffer();

        try {
            long start = System.currentTimeMillis();
            MyLogger.dbNotify("HTTP_REQUEST (" + httpTimeout + ") " + httpAddress + " : " + requestMessage);
            URL u = new URL(httpAddress);
            URLConnection uc = u.openConnection();
            uc.setConnectTimeout(httpTimeout);
            HttpURLConnection connection = (HttpURLConnection)uc;
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-type", "text/xml; charset=utf-8");
            connection.setRequestProperty("Content-Length", String.valueOf(requestMessage.length()));
            connection.setRequestProperty("key", "OxL1MHQkry7OBUwZJcLmhrqpuuimJ5j8");
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
            out.writeBytes(requestMessage);
            out.flush();
            InputStream in = connection.getInputStream();

            int c;
            while((c = in.read()) != -1) {
                sb.append((char)c);
            }

            in.close();
            out.close();
            connection.disconnect();
            MyLogger.dbNotify("HTTP_RESPONSE  (" + (System.currentTimeMillis() - start) + " ms) " + httpAddress + " : " + sb.toString().replace("\r", "").replace("\n", ""));
        } catch (Exception var15) {
            MyLogger.error(var15.getMessage(), var15);
            if (ai.incrementAndGet() <= 10) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException var14) {
                }

                this.sendHttpPost(ai, httpAddress, httpTimeout, requestMessage);
            }
        }

        return sb.toString();
    }
}
