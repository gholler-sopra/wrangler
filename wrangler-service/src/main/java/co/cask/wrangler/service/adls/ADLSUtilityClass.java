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
package co.cask.wrangler.service.adls;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ADLSUtilityClass {

    public static String testConnection(ADLStoreClient client) throws IOException {
        try {
            client.enumerateDirectory("/");
        } catch (IOException e){
            throw new IOException("Connection Failed, please check given credentials : " + e.getMessage());
        }
        return "Success";
    }

    public static JsonObject initClientReturnResponse(ADLStoreClient client, String path) throws IOException {
        if (!client.checkExists(path)) {
            throw new IOException("Given path doesn't exist");
        }
        List<DirectoryEntry> list = client.enumerateDirectory(path);
        JsonArray values = new JsonArray();
        for (DirectoryEntry entry : list) {
            JsonObject object = new JsonObject();
            object.addProperty("name", entry.name);
            object.addProperty("path", entry.fullName);
            object.addProperty("displaySize", entry.length);
            object.addProperty("type", entry.type.toString());
            object.addProperty("group", entry.group);
            object.addProperty("user", entry.user);
            object.addProperty("permission", entry.permission);
            object.addProperty("modifiedTime", entry.lastModifiedTime.toString());
            if (entry.type.equals(DirectoryEntryType.DIRECTORY)) {
                object.addProperty("directory", true);
            } else {
                object.addProperty("directory", false);
            }
            values.add(object);
        }
        JsonObject response = new JsonObject();
        response.add("values", values);
        return response;
    }

    public static InputStream clientInputStream(ADLStoreClient client, FileQueryDetails fileQueryDetails) throws IOException{
        DirectoryEntry file = client.getDirectoryEntry(fileQueryDetails.getFilePath());
        InputStream inputStream = client.getReadStream(file.fullName);
        return inputStream;
    }

    public static DirectoryEntry getFileFromClient(ADLStoreClient client,String path) throws IOException{
        DirectoryEntry file = client.getDirectoryEntry(path);
        return file;
    }

}
