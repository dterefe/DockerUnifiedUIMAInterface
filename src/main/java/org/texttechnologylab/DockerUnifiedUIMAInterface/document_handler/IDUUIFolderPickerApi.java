package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface IDUUIFolderPickerApi {

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
                List<Map<String, Object>> childrenJson = (List<Map<String, Object>>) json.get("children");
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

    DUUIFolder getFolderStructure();

}
