package alluxio.client.file.cache.mt.run;

import org.apache.poi.ss.usermodel.Workbook;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class ExcelTest {
  private static int accessTime = 0;
  private static Sheet mSheet;
  private static CreationHelper createHelper;
  private static Workbook workbook;

  static {
    workbook = new HSSFWorkbook();
    createHelper= workbook.getCreationHelper();
    mSheet= workbook.createSheet();
  }

  public static void addnew(int accsee, double hitRatio1, double HitRatio2, double hitRatio3) {
    accessTime ++;
    Row row = mSheet.createRow(accessTime);

    Cell cell0 = row.createCell((short) 1);
    cell0.setCellValue(accsee);

    Cell cell = row.createCell((short) 2);
    cell.setCellValue(hitRatio1);

    Cell cell2 = row.createCell((short) 3);
    cell2.setCellValue(HitRatio2);

    Cell cell3 = row.createCell((short) 4);
    cell3.setCellValue(hitRatio3);
  }

  public static void generateFile(){
    try {
      String filename = "C:/Users/innkp/Desktop/workbook.xls";
      if (workbook instanceof XSSFWorkbook) {
        filename = filename + "x";
      }
      File f = new File(filename);
      if (f.exists()) {
        f.delete();
      }

      FileOutputStream out = new FileOutputStream(filename);
      workbook.write(out);
      out.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
