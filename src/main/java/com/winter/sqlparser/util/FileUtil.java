package com.winter.sqlparser.util;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.monitorjbl.xlsx.StreamingReader;


public class FileUtil {
    private static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static String readSQL(String path) {
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);
            StringBuilder sb = new StringBuilder();
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                sb.append("\n");
                sb.append(data);
            }
            myReader.close();
            return sb.toString();
            //            return Arrays.stream(sb.toString().trim().split(sqlSplit))
            //                    .filter(StringUtils::isNotEmpty).distinct()
            //                    .collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            logger.error("file not found exception", e);
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> readExcel(String path, int maxLine) throws Exception {
        List<String> res = Lists.newLinkedList();
        try {
            int line = 0;
            FileInputStream in = new FileInputStream(path);
            Workbook workbook = StreamingReader.builder()
                    .rowCacheSize(50000)
                    .bufferSize(40960)
                    .open(in);
            //            Workbook workbook = WorkbookFactory.create(new File(path));
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                //For each row, iterate through all the columns
                Iterator<Cell> cellIterator = row.cellIterator();
                line++;
                if (line > maxLine) {
                    return res;
                }
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    String sql = null;
                    try {
                        sql = cell.getStringCellValue();
                    } catch (Exception e) {
                        //
                    }
                    if (StringUtils.isNotEmpty(sql)) {
                        res.add(sql);
                    }
                }
            }
            //            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }
}
