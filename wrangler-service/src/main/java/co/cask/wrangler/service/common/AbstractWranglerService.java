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
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

public class AbstractWranglerService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHttpServiceHandler.class);
  private static final Gson GSON = new Gson();
  protected ConnectionStore store;
  private static final long UGI_CACHE_EXPIRATION_MS = 3600000L;
  private static LoadingCache<UGICacheKey, UserGroupInformation> ugiCache;

  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table connectionTable;

  @UseDataSet(WORKSPACE_DATASET)
  protected WorkspaceDataset ws;
  
  private Map<String, String> runtimeArgs;

  static {
    ugiCache = createUGICache();
  }
  
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    LOG.debug("Initialize API. Runtime args {}",
        Arrays.toString(context.getRuntimeArguments().entrySet().toArray()));
    runtimeArgs = context.getRuntimeArguments();
    store = new ConnectionStore(connectionTable);
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
                           InterruptedException, ExecutionException {
    UserGroupInformation defaultUgi = null;
    if (!UserGroupInformation.isSecurityEnabled()) {
      LOG.debug("Security is disabled");
      return defaultUgi;
    }
      
    if (runtimeArgs == null || impersonatedUser == null) {
      LOG.debug("Runtime arguments or impersonatedUser is null");
      return defaultUgi;
    }
      
    boolean userImpersonationEnabled = isUserImpersonationEnabled();
    if (!userImpersonationEnabled) {
      LOG.debug("User impersonation is disabled");
      return defaultUgi;
    }
    
    String nsKeytab;
    String nsPrincipal;    
    if ((runtimeArgs.containsKey(PropertyIds.NAMESPACE_KETAB_PATH)) &&
        (runtimeArgs.containsKey(PropertyIds.NAMESPACE_PRINCIPAL_NAME))) {
      nsKeytab = getLocalizedKeytabPath();
      nsPrincipal = runtimeArgs.get(PropertyIds.NAMESPACE_PRINCIPAL_NAME);
    } else {
      LOG.debug("Keytab/Principal not found in Runtime arguments");
      return defaultUgi;
    }

    if (nsKeytab == null || nsPrincipal == null) {
      LOG.debug("Keytab or principal in Runtime arguments is null");
      return defaultUgi;
    }
    
    return ugiCache.get(new UGICacheKey(impersonatedUser, nsKeytab, nsPrincipal));
  }
  
  private static UserGroupInformation createProxyUGI(String impersonatedUser, String keytab,
      String principal) throws IOException, InterruptedException {
    if (impersonatedUser == null || keytab == null || principal == null) {
      return null;
    }
    
    UserGroupInformation currnetUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
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
    if ((runtimeArgs.containsKey(PropertyIds.USER_IMPERSONATION_ENABLED)) &&
              (runtimeArgs.get(PropertyIds.USER_IMPERSONATION_ENABLED).equalsIgnoreCase("true"))) {
      return true;
    }
    return false;
  }
  
  private static LoadingCache<UGICacheKey, UserGroupInformation> createUGICache() {
    long expirationMillis = UGI_CACHE_EXPIRATION_MS;
    return CacheBuilder.newBuilder()
      .expireAfterWrite(expirationMillis, TimeUnit.MILLISECONDS)
      .build(new CacheLoader<UGICacheKey, UserGroupInformation>() {
        @Override
        public UserGroupInformation load(UGICacheKey key) throws Exception {
          return createProxyUGI(key.getImpersonatedUser(), key.getKeytab(), key.getPrincipal());
        }
      });
  }

  private static final class UGICacheKey {
    private final String impersonatedUser;
    private final String keytab;
    private final String principal;

    UGICacheKey(String loggedInUser, String keytab, String principal) {
      this.impersonatedUser = loggedInUser;
      this.keytab = keytab;
      this.principal = principal;
    }

    public String getImpersonatedUser() {
      return impersonatedUser;
    }

    public String getKeytab() {
      return keytab;
    }

    public String getPrincipal() {
      return principal;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UGICacheKey cachekey = (UGICacheKey) o;
      
      if (!impersonatedUser.equals(cachekey.getImpersonatedUser())) {
        return false;
      }
      
      if (!keytab.equals(cachekey.getKeytab())) {
        return false;
      }
      
      if (!principal.equals(cachekey.getPrincipal())) {
        return false;
      }
      
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(impersonatedUser, keytab, principal);
    }
  }
}
