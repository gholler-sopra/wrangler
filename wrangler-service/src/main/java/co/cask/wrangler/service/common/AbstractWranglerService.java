/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.service.common;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceException;
import co.cask.wrangler.proto.Recipe;
import co.cask.wrangler.proto.RequestV1;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

public class AbstractWranglerService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHttpServiceHandler.class);
  private static final Gson GSON = new Gson();
  protected ConnectionStore store;
  private static final long DEFAULT_UGI_CACHE_EXPIRATION_MS = 3600000L;
  private static LoadingCache<String, UserGroupInformation> ugiCache;

  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table connectionTable;

  @UseDataSet(WORKSPACE_DATASET)
  protected WorkspaceDataset ws;
  
  private Map<String, String> runtimeArgs;
  private static String nsKeytab;
  private static String nsPrincipal;
  private static long ugiCacheExpirationMs;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    LOG.debug("Initialize API. Runtime args {}",
        Arrays.toString(context.getRuntimeArguments().entrySet().toArray()));
    runtimeArgs = context.getRuntimeArguments();
    store = new ConnectionStore(connectionTable);
    
    if (!isUserImpersonationEnabled()) {
      return;
    }
    
    if (runtimeArgs == null) {
      throw new Exception("Runtime arguments not found");
    }
    
    if ((runtimeArgs.containsKey(PropertyIds.NAMESPACE_KETAB_PATH)) &&
        (runtimeArgs.containsKey(PropertyIds.NAMESPACE_PRINCIPAL_NAME))) {
      nsKeytab = getLocalizedKeytabPath();
      nsPrincipal = runtimeArgs.get(PropertyIds.NAMESPACE_PRINCIPAL_NAME);
    } else {
      throw new Exception(
          "User impersonation is enabled but valid Keytab/Principal not found in Runtime arguments");
    }

    initialzeUGICache();
  }

  /**
   * Return whether the header needs to be copied when creating the pipeline source for the specified workspace.
   * This just amounts to checking whether parse-as-csv with the first line as a header is used as a directive.
   */
  protected boolean shouldCopyHeader(@Nullable String workspaceId) throws WorkspaceException {
    if (workspaceId == null) {
      return false;
    }
    String data = ws.getData(workspaceId, WorkspaceDataset.REQUEST_COL, DataType.TEXT);
    if (data == null) {
      return false;
    }

    RequestV1 workspaceReq = GSON.fromJson(data, RequestV1.class);
    Recipe recipe = workspaceReq.getRecipe();
    List<String> directives = recipe.getDirectives();
    // yes this is really hacky, but there doesn't seem to be a good way to get the actual directive classes
    return directives.stream()
      .map(String::trim)
      .anyMatch(directive -> directive.startsWith("parse-as-csv") && directive.endsWith("true"));
  }
  
  public UserGroupInformation getProxyUGI(String impersonatedUser) throws IOException,
                InterruptedException, ExecutionException, IllegalArgumentException {
    UserGroupInformation defaultUgi = null;
    if (!UserGroupInformation.isSecurityEnabled()) {
      LOG.debug("Security is disabled");
      return defaultUgi;
    }
        
    boolean userImpersonationEnabled = isUserImpersonationEnabled();
    if (!userImpersonationEnabled) {
      LOG.debug("User impersonation is disabled");
      return defaultUgi;
    }
    
    if (impersonatedUser == null) {
      throw new IllegalArgumentException(
          "User impersonation is enabled but impersonatedUser name is provided as null");
    }
    
    return ugiCache.get(impersonatedUser);
  }
  
  private static UserGroupInformation createProxyUGI(String impersonatedUser)
      throws IOException, InterruptedException {
    if (impersonatedUser == null) {
      return null;
    }
    
    UserGroupInformation currnetUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        nsPrincipal, nsKeytab);
    LOG.debug("current UGI {} , realUser {}, userShortName {} , userName {} , loginUser {}", 
              currnetUgi, currnetUgi.getRealUser(), currnetUgi.getShortUserName(), currnetUgi.getUserName(),
              currnetUgi.getLoginUser());
      
    UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(impersonatedUser, currnetUgi);
    LOG.debug("proxyUgi UGI {} , realUser {}, userShortName {} , userName {} , loginUser {}", 
              proxyUgi, proxyUgi.getRealUser(), proxyUgi.getShortUserName(), proxyUgi.getUserName(),
              proxyUgi.getLoginUser());
    return proxyUgi;
  }

  public String getLocalizedKeytabPath() {
    if (!runtimeArgs.containsKey(PropertyIds.NAMESPACE_KETAB_PATH)) {
      LOG.debug("Keytab config not found in Runtime arguments");
      return null;
    }
      
    String keytabPath = runtimeArgs.get(PropertyIds.NAMESPACE_KETAB_PATH);
    Path p = Paths.get(keytabPath);
    String file = p.getFileName().toString();
    return file;
  }
  
  protected boolean isUserImpersonationEnabled() {
    return ((runtimeArgs.containsKey(PropertyIds.USER_IMPERSONATION_ENABLED)) &&
              (runtimeArgs.get(PropertyIds.USER_IMPERSONATION_ENABLED).equalsIgnoreCase("true")));
  }
  
  private static LoadingCache<String, UserGroupInformation> createUGICache() {
    long expirationMillis = ugiCacheExpirationMs;
    return CacheBuilder.newBuilder()
      .expireAfterWrite(expirationMillis, TimeUnit.MILLISECONDS)
      .build(new CacheLoader<String, UserGroupInformation>() {
        @Override
        public UserGroupInformation load(String key) throws Exception {
          return createProxyUGI(key);
        }
      });
  }

  private void initialzeUGICache() {
    if (ugiCache != null) {
      return;
    }
    
    synchronized (AbstractHttpServiceHandler.class) {
      if (ugiCache != null) {
        return;
      }
      ugiCacheExpirationMs = getUgiCacheExpirationMs();
      ugiCache = createUGICache();
    }
  }
  
  private long getUgiCacheExpirationMs() {
    Configuration conf = new Configuration();
    long tokenRenewInterval = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    long retValue = DEFAULT_UGI_CACHE_EXPIRATION_MS;
    if (tokenRenewInterval > 0) {
      retValue = (tokenRenewInterval / 2);
    }
    LOG.debug("Setting cache expiration time to: {} ms", retValue);
    return retValue;
  }
}
