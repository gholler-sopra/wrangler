 /*
  * Copyright © 2017-2018 Cask Data, Inc.
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

import co.cask.wrangler.dataset.workspace.WorkspaceDataset;

import java.util.Map;
import static co.cask.wrangler.service.common.Constants.*;

public class WorkspaceUtils {

    /**
     * Utility function to get workspace scope
     *
     * @param providedScope
     * @param loggedInUser
     * @param runtimeArgs
     * @return workspaceScope
     */
    public static String getScope(String providedScope, String loggedInUser, Map<String, String> runtimeArgs){
        // first preference is UI/user provided scope
        if(providedScope!=null){
            return providedScope;
        }
        // if impersonation is enabled, use user scope
        if(runtimeArgs.containsKey(ENABLE_USER_IMPERSONATION_CONFIG_KEY)
                && runtimeArgs.get(ENABLE_USER_IMPERSONATION_CONFIG_KEY).equalsIgnoreCase("true")
                && loggedInUser !=null){
            return loggedInUser;
        }
        // option to set default scope at namespace level
        if(runtimeArgs.containsKey(DATAPREP_WORKSPACE_LEVEL_KEY)
                && runtimeArgs.get(DATAPREP_WORKSPACE_LEVEL_KEY).equalsIgnoreCase(WorkspaceDataset.DEFAULT_SCOPE)){
            return WorkspaceDataset.DEFAULT_SCOPE;
        }
        // if request contains user info, use user scope
        if(loggedInUser !=null){
            return loggedInUser;
        }
        // else use default scope
        return WorkspaceDataset.DEFAULT_SCOPE;

    }
}
