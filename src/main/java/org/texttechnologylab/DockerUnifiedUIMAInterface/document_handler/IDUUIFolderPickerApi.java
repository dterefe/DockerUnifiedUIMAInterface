package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface IDUUIFolderPickerApi {
    
    /**
     * Get the configured max concurrency used by this provider while building directory trees.
     * Providers can override to expose current configuration.
     */
    default int getDirectoryTreeMaxConcurrency() {
        return 32;
    }

    /**
     * Configure max concurrency used by this provider while building directory trees.
     * Implementations should clamp values <= 0 to a safe default.
     * Default is a no-op for providers that don't support configuration.
     */
    default void setDirectoryTreeMaxConcurrency(int maxConcurrency) {
        // no-op by default
    }

    /**
     * New folder picker API: unified, rich tree model.
     * Providers are responsible for honoring maxDepth/includeFiles.
     * @throws Exception 
     */
    default DUUIDirectoryNode getDirectoryTree(int maxDepth, boolean includeFiles) throws Exception {
        return getDirectoryTree(null, maxDepth, includeFiles);
    }

    DUUIDirectoryNode getDirectoryTree(DUUIDirectoryNode folder, int maxDepth, boolean includeFiles) throws Exception;

    @Deprecated
    public static class DUUIFolder {

        String id;
        String name;
        List<DUUIFolder> children;

        public DUUIFolder(String id, String name) {
            this.id = id;
            this.name = name;
            this.children = new ArrayList<>();
        }

        public void addChild(DUUIFolder child) {
            children.add(child);
        }

        public Map<String, Object> toJson() {
            Map<String, Object> map = new HashMap<>();

            map.put("id", id);
            map.put("content", name);
            map.put("children", children.stream().map(DUUIFolder::toJson).collect(Collectors.toList()));

            return map;
        }

        public static DUUIFolder fromJson(Map<String, Object> json) {
            String id = (String) json.get("id");
            String name = (String) json.get("content");
            DUUIFolder folder = new DUUIFolder(id, name);

            try {
                List<Map<String, Object>> childrenJson = (List<Map<String, Object>>) json.getOrDefault("children", new ArrayList<Map<String, Object>>());
                for (Map<String, Object> childJson : childrenJson) {
                    folder.addChild(fromJson(childJson));
                }
            } catch (Exception e) {
                // Handle the case where "children" is not a list or is missing
                System.err.println("Error parsing children for folder: " + name + " " + id);
            }

            return folder;
        }
    }

    @Deprecated
    DUUIFolder getFolderStructure();

}
