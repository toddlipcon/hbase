/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.web;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hbase.tmpl.master.StatusTmpl;

/**
 * The servlet responsible for rendering the index page of the
 * master.
 */
public class MasterStatusServlet extends HttpServlet {
  private static final Log LOG = LogFactory.getLog(MasterStatusServlet.class);
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException {
    response.setContentType("text/html");

    HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
    Configuration conf = master.getConfiguration();
    HBaseAdmin admin = new HBaseAdmin(conf);    
        
    Map<String, Integer> frags = getFragmentationInfo(master, conf);
    
    ServerName rootLocation = getRootLocationOrNull(master);
    ServerName metaLocation = master.getCatalogTracker().getMetaLocation();
    List<ServerName> servers = master.getServerManager().getOnlineServersList();
    

    new StatusTmpl()
      .setFrags(frags)
      .setShowAppendWarning(shouldShowAppendWarning(conf))
      .setRootLocation(rootLocation)
      .setMetaLocation(metaLocation)
      .setServers(servers)
      .render(response.getWriter(),
          master, admin);
    
  }

  private ServerName getRootLocationOrNull(HMaster master) {
    try {
      return master.getCatalogTracker().getRootLocation();
    } catch (InterruptedException e) {
      LOG.warn("Unable to get root location", e);
      return null;
    }
  }

  private Map<String, Integer> getFragmentationInfo(
      HMaster master, Configuration conf) throws IOException {
    boolean showFragmentation = conf.getBoolean(
        "hbase.master.ui.fragmentation.enabled", false);    
    if (showFragmentation) {
      return FSUtils.getTableFragmentation(master);
    } else {
      return null;
    }
  }

  static boolean shouldShowAppendWarning(Configuration conf) {
    try {
      return !FSUtils.isAppendSupported(conf) && FSUtils.isHDFS(conf);
    } catch (IOException e) {
      LOG.warn("Unable to determine if append is supported", e);
      return false;
    }
  }
}
