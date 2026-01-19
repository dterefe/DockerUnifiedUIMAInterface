package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.json.JSONArray;
import org.json.JSONObject;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;

/**
 * Handler for reading YouTube video metadata and transcripts (if available).
 * Supports playlists, channels, and individual videos via YouTube Data API.
 * Read-only handler that queries YouTube API and stores URL in a dedicated view.
 */
public final class YouTubeDocumentHandler implements IDUUIDocumentHandler {
    private final HttpClient http;
    private final String apiKey;
    private final String source; // playlist/channel/video URL or id string
    private final boolean addMetadata;
    private final String viewName; // where the url-sofa + yt annotations live

    private final List<YouTubeVideo> videos;

    public YouTubeDocumentHandler(String source, String apiKey, boolean addMetadata, String viewName) throws IOException, InterruptedException {
        this.http = HttpClient.newHttpClient();
        this.apiKey = apiKey;
        this.source = source;
        this.addMetadata = addMetadata;
        this.viewName = viewName == null || viewName.isBlank() ? "_youtube" : viewName;
        this.videos = loadVideoListAndMaybeMetadata();
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) {
        throw new UnsupportedOperationException("YouTubeDocumentHandler is read-only");
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) {
        List<DUUIDocument> docs = new ArrayList<>();
        for (YouTubeVideo v : videos) {
            if (v.id == null || v.id.isBlank()) {
                throw new IllegalStateException("YouTubeDocumentHandler: invalid video id (blank)");
            }
            DUUIDocument d = new DUUIDocument(v.id, "youtube://video/" + v.id);
            d.setMimeType("application/x-duui-youtube");
            docs.add(d);
        }
        return docs;
    }

    @Override
    public DUUIDocument readDocument(String path) {
        String id = path.substring(path.lastIndexOf('/') + 1);
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("YouTubeDocumentHandler: invalid path (missing video id): " + path);
        }
        DUUIDocument d = new DUUIDocument(id, "youtube://video/" + id);
        d.setMimeType("application/x-duui-youtube");
        return d;
    }

    @Override
    public void deserialize(DUUIDocument document, JCas cas, DUUIDocumentReader.DeserializationContext ctx) throws Exception {
        String id = document.getName();
        String url = "https://www.youtube.com/watch?v=" + id;

        // keep prompt/default view clean: store URL as sofa in a dedicated view
        JCas ytView;
        try {
            ytView = cas.getView(viewName);
        } catch (Exception e) {
            ytView = cas.createView(viewName);
        }
        ytView.setSofaDataString(url, "text/x-uri");

        if (!addMetadata) return;
        YouTubeVideo v = findVideo(id);
        if (v == null) return;

        // DocumentMetaData
        if (JCasUtil.select(cas, DocumentMetaData.class).isEmpty()) {
            DocumentMetaData dmd = DocumentMetaData.create(cas);
            dmd.setDocumentId(v.id);
            dmd.setDocumentTitle(v.title);
            dmd.setDocumentUri("https://www.youtube.com/watch?v=" + v.id);
            dmd.addToIndexes();
        }

        // Note: YouTube annotation type not available in typesystem; using DocumentMetaData only.
        // To add more YouTube-specific metadata, create a custom annotation type in typesystem
    }

    private YouTubeVideo findVideo(String id) {
        for (YouTubeVideo v : videos) {
            if (v.id.equals(id)) return v;
        }
        return null;
    }

    private List<YouTubeVideo> loadVideoListAndMaybeMetadata() throws IOException, InterruptedException {
        List<YouTubeVideo> out = new ArrayList<>();

        if (source.contains("&list=")) {
            String playlistId = "";
            for (String p : source.split("&")) {
                if (p.startsWith("list=")) {
                    playlistId = p.substring("list=".length());
                    break;
                }
            }
            String pageToken = "";
            do {
                JSONObject json = getPlaylistVideos(playlistId, pageToken);
                JSONArray items = json.getJSONArray("items");
                List<YouTubeVideo> page = new ArrayList<>();
                for (int i = 0; i < items.length(); i++) {
                    String videoId = items.getJSONObject(i).getJSONObject("contentDetails").getString("videoId");
                    page.add(new YouTubeVideo(videoId));
                }
                if (addMetadata) generateBulkMetadata(page);
                out.addAll(page);
                pageToken = json.has("nextPageToken") ? json.getString("nextPageToken") : "";
            } while (!pageToken.isEmpty());
            return out;
        }

        if (source.contains("watch?v")) {
            String id = source.split("watch\\?v=")[1].split("&")[0];
            YouTubeVideo v = new YouTubeVideo(id);
            if (addMetadata) generateMetadata(v);
            out.add(v);
            return out;
        }

        if (source.contains("youtu.be/")) {
            String id = source.split("youtu\\.be/")[1].split("&")[0];
            YouTubeVideo v = new YouTubeVideo(id);
            if (addMetadata) generateMetadata(v);
            out.add(v);
            return out;
        }

        // channel
        String channelId = null;
        if (source.contains("/@")) {
            channelId = getChannelIdByHandle(source.split("@")[1].split("/")[0]);
        } else if (source.contains("/channel/")) {
            channelId = source.split("/channel/")[1].split("/")[0];
        }
        if (channelId == null) return out;

        String pageToken = "";
        do {
            JSONObject json = getChannelVideosByChannelId(channelId, pageToken);
            JSONArray items = json.getJSONArray("items");
            List<YouTubeVideo> page = new ArrayList<>();
            for (int i = 0; i < items.length(); i++) {
                JSONObject idObj = items.getJSONObject(i).getJSONObject("id");
                if (!idObj.has("videoId")) continue;
                page.add(new YouTubeVideo(idObj.getString("videoId")));
            }
            if (addMetadata) generateBulkMetadata(page);
            out.addAll(page);
            pageToken = json.has("nextPageToken") ? json.getString("nextPageToken") : "";
        } while (!pageToken.isEmpty());

        return out;
    }

    private void generateMetadata(YouTubeVideo video) throws IOException, InterruptedException {
        List<YouTubeVideo> single = new ArrayList<>();
        single.add(video);
        generateBulkMetadata(single);
    }

    private void generateBulkMetadata(List<YouTubeVideo> vids) throws IOException, InterruptedException {
        if (vids.isEmpty()) return;
        String joined = vids.stream().map(v -> v.id).collect(Collectors.joining(","));
        JSONObject info = getVideoInformation(joined);
        JSONArray items = info.getJSONArray("items");
        for (int i = 0; i < items.length(); i++) {
            JSONObject it = items.getJSONObject(i);
            String id = it.getString("id");
            YouTubeVideo v = null;
            for (YouTubeVideo x : vids) if (x.id.equals(id)) { v = x; break; }
            if (v == null) continue;
            v.applyMetadata(
                it.getJSONObject("snippet"),
                it.getJSONObject("statistics"),
                it.getJSONObject("contentDetails")
            );
        }
    }

    private JSONObject getPlaylistVideos(String playlistId, String pageToken) throws IOException, InterruptedException {
        String url = "https://youtube.googleapis.com/youtube/v3/playlistItems"
            + "?part=contentDetails&playlistId=" + playlistId
            + "&key=" + apiKey + "&maxResults=50"
            + (pageToken.isEmpty() ? "" : "&pageToken=" + pageToken);
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return new JSONObject(resp.body());
    }

    private JSONObject getVideoInformation(String idsCsv) throws IOException, InterruptedException {
        String url = "https://youtube.googleapis.com/youtube/v3/videos"
            + "?part=snippet,statistics,contentDetails&id=" + idsCsv
            + "&key=" + apiKey;
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return new JSONObject(resp.body());
    }

    private String getChannelIdByHandle(String handle) throws IOException, InterruptedException {
        String url = "https://youtube.googleapis.com/youtube/v3/search"
            + "?part=snippet&maxResults=1&q=" + handle
            + "&type=channel&key=" + apiKey;
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        JSONObject json = new JSONObject(resp.body());
        JSONArray items = json.getJSONArray("items");
        if (items.length() == 0) return null;
        return items.getJSONObject(0).getJSONObject("id").getString("channelId");
    }

    private JSONObject getChannelVideosByChannelId(String channelId, String pageToken) throws IOException, InterruptedException {
        String url = "https://www.googleapis.com/youtube/v3/search"
            + "?key=" + apiKey
            + "&channelId=" + channelId
            + "&part=id&order=date&maxResults=50"
            + (pageToken.isEmpty() ? "" : "&pageToken=" + pageToken);
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return new JSONObject(resp.body());
    }

    private static int youtubeDateToInt(String youtubeDate) {
        String[] dateElements = youtubeDate.split("T")[0].split("-");
        String yyyy = dateElements[0];
        String mm = dateElements[1].length() == 1 ? "0" + dateElements[1] : dateElements[1];
        String dd = dateElements[2].length() == 1 ? "0" + dateElements[2] : dateElements[2];
        return Integer.parseInt(dd + mm + yyyy);
    }

    private static int parseIsoDurationSeconds(String duration) {
        // expects "PT#H#M#S" variants
        String d = duration.startsWith("PT") ? duration.substring(2) : duration;
        int total = 0;
        int h = d.indexOf('H');
        if (h >= 0) {
            total += Integer.parseInt(d.substring(0, h)) * 3600;
            d = d.substring(h + 1);
        }
        int m = d.indexOf('M');
        if (m >= 0) {
            total += Integer.parseInt(d.substring(0, m)) * 60;
            d = d.substring(m + 1);
        }
        int s = d.indexOf('S');
        if (s >= 0) {
            total += Integer.parseInt(d.substring(0, s));
        }
        return total;
    }

    private static final class YouTubeVideo {
        final String id;
        String title = "";
        String channelName = "";
        String channelUrl = "";
        int durationSeconds = 0;
        int views = 0;
        int likes = 0;
        int createDateInt = 0;

        YouTubeVideo(String id) { this.id = id; }

        void applyMetadata(JSONObject snippet, JSONObject statistics, JSONObject contentDetails) {
            this.title = snippet.optString("title", "");
            this.channelName = snippet.optString("channelTitle", "");
            this.channelUrl = "https://www.youtube.com/channel/" + snippet.optString("channelId", "");
            this.durationSeconds = parseIsoDurationSeconds(contentDetails.optString("duration", "PT0S"));
            this.views = Integer.parseInt(statistics.optString("viewCount", "0"));
            this.likes = Integer.parseInt(statistics.optString("likeCount", "0"));
            this.createDateInt = youtubeDateToInt(snippet.optString("publishedAt", "1970-01-01T00:00:00Z"));
        }
    }

    @Override
    public void shutdown() {
        // no resources to clean up
    }
}
