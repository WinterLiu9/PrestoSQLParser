package com.winter.sqlparser;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.winter.sqlparser.util.FileUtil;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestUtil {
    List<String> sqls;

    @Before
    public void init() throws Exception {
        String path2SQL = "./src/test/resources/sql/orderv4.xlsx";
        sqls = FileUtil.readExcel(path2SQL, 10000);
    }

    @Test
    public void test() {
        String regex = "[\\t\\r\\n]|(--[^\\r\\n]*)|(/\\*[\\w\\W]*?(?=\\*/)\\*/)";
        Map<String, String> m = Maps.newLinkedHashMap();
        for (String sql : sqls) {
            //            System.out.println(sql);
            //            System.out.println(sql.replaceAll(regex, "\n"));
            m.put(sql, sql.replaceAll(regex, "\n"));
        }
        System.out.println();
    }

    @Test
    public void test1() {
        String view = "create or replace view  ${SCHEMA}.dwd_shop_penalty_punishment_df__%s_s0_${ENV_TAG} \n"
                + "as\n"
                + "select  *\n"
                + "from    ${SCHEMA}.dwd_shop_penalty_punishment_df__reg_s0_${ENV_TAG}\n"
                + "where   tz_type = 'local'\n"
                + "  and   grass_region = '%s'\n"
                + ";";

        String dropView = "drop view if exists ${SCHEMA}.dwd_shop_penalty_punishment_df__%s_s0_${ENV_TAG};";
        String[] regions = new String[] {"MY", "PH", "SG", "TH", "TW", "VN", "BR", "ID", "MX", "CO", "CL", "AR", "PL"};

        //        String[] regions = new String[] {"", "", "", "", "", "", "BR", "", "", "", "", "AR", ""};
        //        ID,SG,TW,PL,TH,MY,
        //                MX,CO,CL,VN,PH
        for (String region : regions) {
            System.out.println(String.format(dropView, region.toLowerCase(Locale.ROOT), region));
            System.out.println();
        }

    }

    @Test
    public void test2() {
        String m =
                "shop_id, merchant_id, registration_timestamp, registration_datetime, registration_mode, update_time,"
                        + " update_datetime, mapping_status, shop_type, creator, shop_region, is_seller";
        List<String> collect = Arrays.stream(m.split(",")).map(x -> x.trim()).collect(Collectors.toList());
        for (String s : collect) {
            System.out.println(String.format("dev.%s != live.%s and", s, s));
        }
    }

    @Test
    public void test3() {
        String t = "    ,penalty_record_id bigint comment 'primary key'\n"
                + "    ,shop_id bigint comment 'id of shop'\n"
                + "    ,shop_name string comment 'shop name of the seller'\n"
                + "    ,user_id bigint comment 'user id of the seller'\n"
                + "    ,shop_penalty_point bigint comment 'penalty points of the shop'\n"
                + "    ,violation_type bigint comment 'Violation type id'\n"
                + "    ,violation_reason bigint comment 'Violation reason id'\n"
                + "    ,violation_reason_description string comment 'Description of the violation reason'\n"
                + "    ,system_execute_task_date string comment 'T-1 date when backend system calculates metrics & "
                + "decides how many penalty points to assign to shop'\n"
                + "    ,create_timestamp bigint comment 'Timestamp of which log record was created'\n"
                + "    ,create_datetime string\n"
                + "    ,update_timestamp bigint comment 'Timestamp of which log record was updated'\n"
                + "    ,update_datetime string\n"
                + "    ,auto_status int comment 'Whether changes have been automatically made by the backend system'\n"
                + "    ,manual_status int comment 'Whether changes have been made manually'\n"
                + "    ,valid_status bigint comment 'Whether record is valid or not; 1 = valid, 0 = invalid'\n"
                + "    ,metrics_id bigint comment 'id of metrics'\n"
                + "    ,cancel_reason_code int comment 'provides cancellation reason if the record was cancelled'";
        List<String> c =
                Arrays.stream(t.split("\n")).map(x -> x.trim().split(" ")[0].substring(1)).collect(Collectors.toList());
        for (String s : c) {
            //            System.out.println(String.format("COALESCE(di.%s, df.%s) as %s,", s, s, s));
            System.out.println(s + ",");
        }
    }

    @Test
    public void test4() {
        String c = "SG,ID,TW,TH,MY,MX,\n"
                + "CO,CL,VN,PH,BR";
        Set<String> i = Arrays.stream(c.split(",")).map(x -> x.trim()).collect(Collectors.toSet());
        String[] regions = new String[] {"MY", "PH", "SG", "TH", "TW", "VN", "BR", "ID", "MX", "CO", "CL", "AR", "PL"};
        Set<String> r = Arrays.stream(regions).collect(Collectors.toSet());
        for (String s : r) {
            if (!i.contains(s)) {
                System.out.println(s);
            }
        }
    }


    @Test
    public void test5() {

        String oct19 = "7.14 GB\n"
                + "4.66 MB\n"
                + "55.57 MB\n"
                + "34.94 MB\n"
                + "267.95 MB\n"
                + "47.5 MB\n"
                + "1.51 MB\n"
                + "14 MB\n"
                + "3.34 GB\n"
                + "1.55 GB";

        double size19 = getSize(oct19);

        System.out.println(size19);


    }

    private double getSize(String s) {
        List<Double> collect = Arrays.stream(s.split("\\n")).map(x -> {
                    if (x.contains("MB")) {
                        return Double.parseDouble(x.trim().substring(0, x.length() - 2).trim()) / 1024;
                    } else {
                        return Double.parseDouble(x.trim().substring(0, x.length() - 2).trim());
                    }
                })
                .collect(Collectors.toList());
        double res = 0;
        for (Double a : collect) {
            res += a;
        }
        return res;
    }

   
}
