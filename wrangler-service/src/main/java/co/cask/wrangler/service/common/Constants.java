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

public class Constants {

    public static final String USER_ID_KEY = "CDAP-UserId";

    public static final String ENABLE_USER_IMPERSONATION_CONFIG_KEY = "system.user.impersonation.enabled";

    public static final String DATAPREP_WORKSPACE_LEVEL_KEY = "dataprep.workspace.scope";

    public static class Hive{

        public static final String HIVE_ENABLE_ADDING_SCHEMA = "system.append.hive.schema.enabled";

        public static final String HIVE_REMOVE_TABLENAME_FROM_COLUMN_NAME_CONFIG_KEY = "hive.resultset.use.unique.column.names";
    }


}
