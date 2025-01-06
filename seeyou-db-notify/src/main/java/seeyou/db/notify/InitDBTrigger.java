package seeyou.db.notify;

import com.seeyou.dieselpoint.norm.Database;
import com.seeyou.logging.MyLogger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Septian Yudhistira
 * @version 1.0
 * @since 2025-01-06
 */
public class InitDBTrigger {
    public static String DB_NOTIFY_NAME = "balantika_db_notify";

    public void createTrigger(Database database) throws Exception {
        String functionName = "balantika_notify_all_table";
        String dbNotifyName = database.getValue("SELECT n.nspname AS function_schema, p.proname AS function_name FROM pg_proc p LEFT JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND p.proname = 'balantika_notify_all_table' ");
        if (dbNotifyName == null || dbNotifyName.isEmpty()) {
            String templateFunction = "CREATE OR REPLACE FUNCTION balantika_notify_all_table()\nRETURNS trigger AS $$\nDECLARE\n  current_row RECORD;\nBEGIN\n  IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN\n    current_row := NEW;\n  ELSE\n    current_row := OLD;\n  END IF;\n  PERFORM pg_notify(\n    '" + DB_NOTIFY_NAME + "',\n    json_build_object(\n      'p_tbl', TG_TABLE_NAME,\n      'p_id', current_row.id,\n      'p_type', TG_OP,\n\t  'p_datetime', now()::timestamp(6)\n    )::text\n  );\n  RETURN current_row;\nEND;\n$$ LANGUAGE plpgsql;\n\n";
            database.executeSql(templateFunction);
            MyLogger.dbNotify("Create function : " + DB_NOTIFY_NAME);
        }

        List<Map<String, String>> executeSql = database.executeSql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
        Iterator var5 = executeSql.iterator();

        while(true) {
            String tableName;
            String triggerName;
            String dbTrgName;
            do {
                if (!var5.hasNext()) {
                    return;
                }

                Map<String, String> map = (Map)var5.next();
                tableName = (String)map.get("TABLE_NAME");
                triggerName = this.getTriggerName(tableName);
                dbTrgName = database.getValue("SELECT trigger_name from information_schema.triggers WHERE  trigger_name = '" + triggerName + "' LIMIT 1");
            } while(dbTrgName != null && !dbTrgName.isEmpty());

            MyLogger.dbNotify("Create Trigger : " + triggerName);
            String templateTrigger = this.getTableTrigger(triggerName, tableName, "balantika_notify_all_table");
            database.executeSql(templateTrigger);
        }
    }

    public String getTableTrigger(String triggerName, String triggerTable, String functionName) {
        String templateTrigger = "CREATE TRIGGER " + triggerName + "\nAFTER INSERT OR UPDATE OR DELETE\nON " + triggerTable + "\nFOR EACH ROW\nEXECUTE PROCEDURE " + functionName + "();\n\n";
        return templateTrigger;
    }

    public String getTriggerName(String triggerTable) {
        String triggerName = "notify_" + triggerTable + "_trg";
        return triggerName;
    }
}
