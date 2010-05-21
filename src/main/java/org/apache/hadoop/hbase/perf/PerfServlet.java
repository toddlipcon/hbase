package org.apache.hadoop.hbase.perf;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PerfServlet extends HttpServlet {

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    
    PrintWriter out = new PrintWriter(response.getOutputStream());
    PerfCounters.get().dump(out);
    out.close();
  }

  private static final long serialVersionUID = 1L;

}
