package simpleclient;

import java.sql.*;
import java.util.Scanner;

import simpledb.jdbc.StatementAdapter;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.jdbc.network.NetworkDriver;
import simpledb.plan.Plan;

public class SimpleIJ {
   public static void main(String[] args) {
      Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
      String s = sc.nextLine();
      Driver d = (s.contains("//")) ? new NetworkDriver() : new EmbeddedDriver();

      try (Connection conn = d.connect(s, null); Statement stmt = conn.createStatement()) {
         System.out.print("\nSQL> ");
         while (sc.hasNextLine()) {
            // process one line of input
            String cmd = sc.nextLine().trim();
            boolean isExit = execute(stmt, cmd);
            if (isExit) break;
            System.out.print("\nSQL> ");
         }
      } catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }

   public static boolean execute(Statement stmt, String cmd) {
      if (cmd.startsWith("exit")) return true;
      else if (cmd.startsWith("select")) doQuery(stmt, cmd);
      else if (cmd.startsWith("explain")) doExplain(stmt, cmd);
      else doUpdate(stmt, cmd);
      return false;
   }

   private static void doQuery(Statement stmt, String cmd) {
      try (ResultSet rs = stmt.executeQuery(cmd)) {
         ResultSetMetaData md = rs.getMetaData();
         int numcols = md.getColumnCount();
         int totalwidth = 0;

         // print header
         for (int i = 1; i <= numcols; i++) {
            String fldname = md.getColumnName(i);
            int width = md.getColumnDisplaySize(i);
            totalwidth += width;
            String fmt = "%" + width + "s";
            System.out.format(fmt, fldname);
         }
         System.out.println();
         for (int i = 0; i < totalwidth; i++)
            System.out.print("-");
         System.out.println();

         // print records
         while (rs.next()) {
            for (int i = 1; i <= numcols; i++) {
               String fldname = md.getColumnName(i);
               int fldtype = md.getColumnType(i);
               String fmt = "%" + md.getColumnDisplaySize(i);
               if (fldtype == Types.INTEGER) {
                  int ival = rs.getInt(fldname);
                  System.out.format(fmt + "d", ival);
               } else {
                  String sval = rs.getString(fldname);
                  System.out.format(fmt + "s", sval);
               }
            }
            System.out.println();
         }
      } catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }

   private static void doUpdate(Statement stmt, String cmd) {
      try {
         int howmany = stmt.executeUpdate(cmd);
         System.out.println(howmany + " records processed");
      } catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }

   private static void doExplain(Statement stmt, String cmd) {
      try {
         Plan plan = ((StatementAdapter) stmt).getQueryPlan(cmd);
         System.out.println(plan);
      } catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }
}