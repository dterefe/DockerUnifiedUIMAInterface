---
name: dua-service-maintenance
description: Maintain the current BioFID DUA service. Use when fixing DUAService, DUAUceApiSupport, DUAWebSocketServer, DUAServiceLauncher, DUA API dashboard, UCE DUA web websocket/API wiring, DUA container rebuilds, port forwarding, or verification of the running BioFID heideltime DUA service without restarting the importer.
---

# DUA Service Maintenance

Use this workflow for the current BioFID heideltime DUA deployment.

## Current Layout

DUA repo:

```bash
/home/stud_homes/s0424382/projects/ttlab/duui-alpha/DockerUnifiedUIMAInterface
```

UCE repo:

```bash
/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/UCE
```

DUA store:

```bash
/storage/projects/BIOfid/code/dterefe/artifacts/dua-heideltime
```

Source XMI dataset:

```bash
/storage/projects/BIOfid/code/dterefe/artifacts/development/2026_06_18_heideltime
```

Container/image names:

```text
DUA service:  dua-biofid-heideltime-service-17876
DUA importer: dua-biofid-heideltime-importer-full
UCE DUA web:  uce-dua-web-live
DUA image:    localhost/duas:biofid-local
```

## Hard Rule

Do not restart, remove, or recreate `dua-biofid-heideltime-importer-full` for service/dashboard/API fixes.

Only restart `dua-biofid-heideltime-service-17876` after DUA service code changes.

Before any service/container action, check importer status and preserve it if it is running:

```bash
podman ps --format '{{.Names}} {{.ID}} {{.Status}}' | rg '^dua-biofid-heideltime-importer-full\\b'
```

If this prints `Up ...`, do not stop it, do not remove it, and do not start a replacement importer.

If service work needs a container cleanup, target only:

```bash
dua-biofid-heideltime-service-17876
```

Never use broad commands such as:

```bash
podman stop $(podman ps -q)
podman rm -f $(podman ps -aq)
podman compose down
```

## Importer Status And Recovery

Check whether the importer is running:

```bash
podman ps -a --format '{{.Names}} {{.ID}} {{.Status}}' | rg '^dua-biofid-heideltime-importer-full\\b'
```

Running state looks like:

```text
dua-biofid-heideltime-importer-full <id> Up ...
```

Exited state looks like:

```text
dua-biofid-heideltime-importer-full <id> Exited ...
```

Inspect importer logs without attaching or interrupting:

```bash
podman logs --tail 120 dua-biofid-heideltime-importer-full
```

Watch logs without sending input:

```bash
podman logs -f --tail 120 dua-biofid-heideltime-importer-full
```

Leave log follow with the terminal interrupt only if needed; do not stop the container.

Check current import progress through the service:

```bash
curl -sS http://127.0.0.1:17876/stats | node -e "let s='';process.stdin.on('data',d=>s+=d);process.stdin.on('end',()=>{let j=JSON.parse(s); console.log(JSON.stringify({accepted:j.acceptedDocuments,durable:j.durableDocuments,total:j.totalDocuments,failed:j.failedDocuments,pending:j.writebackPendingDocuments},null,2))})"
```

If the importer is exited, first determine why:

```bash
podman inspect dua-biofid-heideltime-importer-full --format 'ExitCode={{.State.ExitCode}} FinishedAt={{.State.FinishedAt}} Error={{.State.Error}}'
podman logs --tail 240 dua-biofid-heideltime-importer-full
```

If logs show malformed XMI such as `SAXParseException`, the importer code should treat it as a failed/skipped document and continue. If the container exited, fix `DUAXMIReader`/importer failure handling first, rebuild the same image, then resume against the existing store.

If logs show direct-memory failure from XMI deserialization, keep it document-local in `DUAXMIReader`; do not wipe the DUA store.

To resume after an exited importer:

1. Do not delete `/storage/projects/BIOfid/code/dterefe/artifacts/dua-heideltime`.
2. Do not start from an empty target.
3. Reuse the same source and target paths.
4. Recreate only `dua-biofid-heideltime-importer-full`.
5. Preserve continuation/resume behavior from the importer implementation.

Before recreating an exited importer, inspect its original command and mounts:

```bash
podman inspect dua-biofid-heideltime-importer-full --format 'Image={{.ImageName}} Cmd={{json .Config.Cmd}} Entrypoint={{json .Config.Entrypoint}}'
podman inspect dua-biofid-heideltime-importer-full --format '{{range .Mounts}}{{.Source}}:{{.Destination}}:{{.RW}} {{end}}'
podman inspect dua-biofid-heideltime-importer-full --format '{{range .Config.Env}}{{.}}{{"\\n"}}{{end}}'
```

Use those values to recreate the importer only after confirming it is not running. Do not invent a different importer command when the original command is available from `podman inspect`.

## Primary Files

DUA service/dashboard:

```bash
duui-dua/dua-core/src/main/java/org/texttechnologylab/duui/dua/service/DUAService.java
```

UCE query/API compatibility layer:

```bash
duui-dua/dua-core/src/main/java/org/texttechnologylab/duui/dua/service/DUAUceApiSupport.java
```

WebSocket server:

```bash
duui-dua/dua-core/src/main/java/org/texttechnologylab/duui/dua/service/DUAWebSocketServer.java
```

Launcher:

```bash
duui-dua/dua-core/src/main/java/org/texttechnologylab/duui/dua/service/DUAServiceLauncher.java
```

Containerfile:

```bash
duui-dua/Containerfile
```

UCE DUA web client/proxy:

```bash
uce.portal/uce-dua-web/src/lib/clients/duaClient.ts
uce.portal/uce-dua-web/src/lib/clients/requestHandler.ts
uce.portal/uce-dua-web/server.mjs
uce.portal/uce-dua-web/vite.config.ts
```

UCE DUA web Svelte surfaces:

```bash
uce.portal/uce-dua-web/src/App.svelte
uce.portal/uce-dua-web/src/lib/components/CorpusSelector.svelte
uce.portal/uce-dua-web/src/lib/components/DuaVizView.svelte
uce.portal/uce-dua-web/src/lib/components/LexiconView.svelte
uce.portal/uce-dua-web/src/lib/components/SearchView.svelte
uce.portal/uce-dua-web/src/lib/components/DocumentReader.svelte
```

Legacy UCE web sources, used as behavior/reference material only:

```bash
uce.portal/uce-web
```

When migrating a legacy view, read the relevant legacy Freemarker/template/server path first, then implement the Svelte version in `uce.portal/uce-dua-web`. The goal is not a literal clone. The goal is to migrate and improve the visualization while preserving the useful data granularity and interaction semantics from legacy UCE.

## UCE DUA Web Troubleshooting Flow

Use this flow when the UCE DUA web UI is empty, slow, disconnected, showing stale values, or using the wrong payload fields.

1. Verify the current service and web ports before editing:

```bash
curl -fsS http://127.0.0.1:17876/health
curl -sS --max-time 5 http://127.0.0.1:4579/runtime-config.js
podman ps --format '{{.Names}} {{.ID}} {{.Status}} {{.Ports}}' | rg 'dua-biofid-heideltime|uce-dua-web-live'
```

2. Verify the exact API shape the frontend is consuming. Do not infer from TypeScript types alone:

```bash
curl -sS 'http://127.0.0.1:17876/dua/api/v1/typesystem?includeAll=true&includeFeatures=true'
curl -sS 'http://127.0.0.1:17876/dua/api/v1/feature-structures?limit=5&offset=0&includeValues=true'
```

3. Keep component code behind the request handler. Components should call the request/client layer, not invent their own endpoint-specific paging, filtering, fallback parsing, or direct service workarounds. Centralize request paging, streaming, normalization, feature-value merging, and compatibility behavior in:

```bash
uce.portal/uce-dua-web/src/lib/clients/requestHandler.ts
uce.portal/uce-dua-web/src/lib/clients/duaClient.ts
```

4. If the API endpoint does not contain the format, value, feature, hierarchy, paging metadata, or resolver output required by the UI, read the DUA service code and fix the endpoint or add the correct endpoint support. Do not hide missing backend capability with frontend guesses, hard-coded type labels, synthetic IDs, unbounded fetches, or type-code fallbacks in components.

Primary backend files for UCE API shapes:

```bash
duui-dua/dua-core/src/main/java/org/texttechnologylab/duui/dua/service/DUAUceApiSupport.java
duui-dua/dua-core/src/main/java/org/texttechnologylab/duui/dua/service/DUAService.java
duui-dua/dua-core/src/main/java/org/texttechnologylab/duui/dua/service/DUAWebSocketServer.java
```

5. After backend endpoint changes, rebuild the DUA image and restart only `dua-biofid-heideltime-service-17876`. Do not restart or remove the importer for web/API fixes.

6. After frontend changes, run:

```bash
cd /home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/UCE/uce.portal/uce-dua-web
npm run check
npm run build
```

If `uce-dua-web-live` is serving Vite dev mode, source edits may be picked up live. If it is serving a built bundle, rebuild/recreate only the UCE web container and keep the DUA service/importer unchanged unless the API changed.

7. Verify rendered behavior with browser evidence when the user asks for UI proof. Use the Playwright skill workflow when available. A clean build is not proof that the visible UI is filled, connected, or using the correct feature values.

## UCE Web Data Rules

The UCE DUA web should display UIMA/DUA payloads as feature structures and feature values.

Do:

```text
Use FeatureStructure records, fsRef, typeName, features, featureValues, ranges, and UIMA type hierarchy.
Use artifact title/artifactId/sourceURI/metadataJson from feature values when showing artifacts.
Use paging/streaming at request-handler or backend endpoint level for large feature-structure collections.
Use explicit type-system requests such as includeAll=true when hierarchy roots are required.
```

Do not:

```text
Do not use arbitrary frontend IDs or type-code-only state as user-visible identity.
Do not use generic label/name fallbacks when an artifact title or feature value is required.
Do not fetch the entire corpus into a component to compensate for a missing paged endpoint.
Do not hard-code BioFID hierarchy in components when the type system can provide parent/child relations.
Do not display abstract implementation roots such as Artifact when the user-facing hierarchy should start at Corpus and Document.
```

When a legacy UCE view had richer data granularity than the current DUA endpoint, the correct fix is to inspect the legacy source, inspect the current DUA service/source, then modify the API/service normalization so the Svelte frontend can request the same useful data cleanly.

## Build DUA

From the DUA repo:

```bash
cd /home/stud_homes/s0424382/projects/ttlab/duui-alpha/DockerUnifiedUIMAInterface
mvn -pl duui-dua/dua-core -am -DskipTests package
```

Require `BUILD SUCCESS`. This package step must refresh:

```bash
duui-dua/dua-core/target/dua-core-*.jar
duui-dua/dua-core/target/dependency
```

The container copies both paths, so stale dependencies can cause runtime errors.

## Rebuild Image

From the DUA repo:

```bash
podman build -t localhost/duas:biofid-local -f duui-dua/Containerfile duui-dua
```

Require:

```text
Successfully tagged localhost/duas:biofid-local
```

## Restart Only Service Container

First verify importer is alive:

```bash
podman ps --format '{{.Names}} {{.ID}} {{.Status}} {{.Ports}}' | rg 'dua-biofid-heideltime|uce-dua-web-live'
```

Restart only the service:

```bash
podman rm -f dua-biofid-heideltime-service-17876

podman run -d \
  --name dua-biofid-heideltime-service-17876 \
  -p 17876:17875 \
  -p 17877:17876 \
  -v /storage/projects/BIOfid/code/dterefe/artifacts/dua-heideltime:/data/dua:rw \
  -v /storage/projects/BIOfid/code/dterefe/artifacts/development/2026_06_18_heideltime:/storage/projects/BIOfid/code/dterefe/artifacts/development/2026_06_18_heideltime:ro \
  -e DUA_STORE_PATH=/data/dua \
  -e DUA_PORT=17875 \
  -e DUA_SERVICE_BIND_HOST=0.0.0.0 \
  -e DUA_SERVICE_PUBLIC_HOST=127.0.0.1 \
  -e SPARQL_HOST=http://uce-fuseki-sparql:5430/ \
  -e SPARQL_ENDPOINT=biofid-search/sparql \
  -e RAG_WEBSERVER_BASE_URL=http://uce-rag-service:5678/ \
  -e EMBEDDING_WEBSERVER_BASE_URL=http://uce-rag-service:5678/ \
  -e DUA_RAG_MODEL_URL=https://www.llm.texttechnologylab.org/api \
  -e DUA_RAG_MODEL='openai/gemma3:latest' \
  -e DUA_RAG_API_KEY= \
  localhost/duas:biofid-local
```

## Ports And URLs

Container ports:

```text
17875 = DUA HTTP
17876 = DUA WebSocket
```

Host ports:

```text
http://localhost:17876 -> container HTTP 17875
ws://localhost:17877   -> container WebSocket 17876
```

Dashboards:

```text
DUA API dashboard: http://localhost:17876/
UCE DUA web:       http://localhost:4579/
```

Codex browser may use a forwarded local port such as:

```text
http://localhost:52613/
```

When the browser is on a forwarded UCE port, verify:

```bash
curl -sS --max-time 5 http://127.0.0.1:<forwarded-port>/runtime-config.js
```

Expected:

```js
window.uceDuaWsUrl = "/ws/duaviz";
window.uceDuavizWsUrl = "/ws/duaviz";
```

## Verify Service

Health:

```bash
curl -fsS http://127.0.0.1:17876/health
```

Stats:

```bash
curl -sS http://127.0.0.1:17876/stats | node -e "let s='';process.stdin.on('data',d=>s+=d);process.stdin.on('end',()=>{let j=JSON.parse(s); console.log(JSON.stringify({accepted:j.acceptedDocuments,durable:j.durableDocuments,total:j.totalDocuments,failed:j.failedDocuments,pending:j.writebackPendingDocuments,hasError:Object.hasOwn(j,'error')},null,2))})"
```

Expected shape:

```json
{"accepted":2804,"durable":2803,"total":2823,"failed":14,"pending":1,"hasError":false}
```

Direct WebSocket:

```bash
node - <<'NODE'
const ws = new WebSocket('ws://127.0.0.1:17877/');
const started = Date.now();
ws.addEventListener('open', () => ws.send(JSON.stringify({ action: 'document.list', limit: 5 })));
ws.addEventListener('message', e => {
  const msg = JSON.parse(e.data);
  console.log(JSON.stringify({
    ms: Date.now() - started,
    ok: msg.ok,
    status: msg.status,
    docs: msg.result?.documents?.length ?? msg.documents?.length,
    total: msg.result?.total ?? msg.total,
    error: msg.error || null
  }, null, 2));
  process.exit(0);
});
ws.addEventListener('error', e => {
  console.error(e.message || e);
  process.exit(1);
});
setTimeout(() => {
  console.error('TIMEOUT');
  process.exit(2);
}, 10000);
NODE
```

UCE web proxy WebSocket, replacing `<forwarded-port>` with the in-app browser port:

```bash
node - <<'NODE'
const ws = new WebSocket('ws://127.0.0.1:<forwarded-port>/ws/duaviz');
const started = Date.now();
ws.addEventListener('open', () => ws.send(JSON.stringify({ action: 'document.list', limit: 5 })));
ws.addEventListener('message', e => {
  const msg = JSON.parse(e.data);
  console.log(JSON.stringify({
    ms: Date.now() - started,
    ok: msg.ok,
    status: msg.status,
    docs: msg.result?.documents?.length ?? msg.documents?.length,
    total: msg.result?.total ?? msg.total,
    error: msg.error || null
  }, null, 2));
  process.exit(0);
});
ws.addEventListener('error', e => {
  console.error(e.message || e);
  process.exit(1);
});
setTimeout(() => {
  console.error('TIMEOUT');
  process.exit(2);
}, 10000);
NODE
```

Before using this snippet, replace `<forwarded-port>` with the actual numeric port.

## Common Diagnosis

If the UI shows a stale `SAXParseException`, distinguish two cases:

```text
failedDocuments increased = malformed XMI was skipped as a document-local failure
stats.hasError true      = service is incorrectly exposing document-local failure as top-level status error
```

If `document.list` times out:

1. Test direct `ws://127.0.0.1:17877/`.
2. Test UCE proxy `/ws/duaviz`.
3. Inspect `DUAUceApiSupport.documentList`.
4. Avoid unbounded directory scans or serializing huge pages during startup.
5. Keep normal list responses paged.

## Active DUA Store Migration

Use this when the DUA store itself must be corrected in place, for example when imported annotation spans were written against side/type-specific views and must be collapsed into the document artifact's `_InitialView`.

Hard rules:

```text
do not run a migration while the importer is writing
do not start the importer while the migration is writing
do not delete the store
do not use ad hoc frontend fallbacks to hide bad DUA data
do not collapse across domain bases
dry-run the exact store first
apply only after unresolved counts are zero or explicitly understood
```

First check processes:

```bash
podman ps -a --format '{{.Names}} {{.ID}} {{.Status}}' | rg '^dua-biofid-heideltime-importer-full\b|^dua-biofid-heideltime-service-17876\b'
pgrep -af 'BIOfidAnnotationViewMigration|BIOfidPipelineImporter|DUAServiceLauncher' || true
```

If `dua-biofid-heideltime-importer-full` is `Up`, do not run an in-place migration. Let it finish or explicitly stop it only when the user asks. The DUA service may be left running for read-only dry-runs, but restart it after an apply so readers reopen the corrected LMDB state.

Compile migration-capable code:

```bash
cd /home/stud_homes/s0424382/projects/ttlab/duui-alpha/DockerUnifiedUIMAInterface
mvn -pl duui-dua/dua-core -am -DskipTests package
```

Run Java LMDB tools with the same module access flags used by the container:

```bash
JAVA_LMDB_FLAGS='--add-opens=java.base/java.nio=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED'
CP='duui-dua/dua-core/target/classes:duui-dua/dua-core/target/dependency/*:duui-base/target/classes:duui-core/target/classes'
```

For annotation side-view collapse, dry-run a single shard first:

```bash
java $JAVA_LMDB_FLAGS -cp "$CP" \
  org.texttechnologylab.duui.dua.uce.BIOfidAnnotationViewMigration \
  /storage/projects/BIOfid/code/dterefe/artifacts/dua-heideltime/domains/biofid-2026-06-18-heideltime-group-0
```

Expected safe dry-run shape:

```text
seenSpans=N
migratedSpans=N
unresolvedSpans=0
applied=false
```

Then dry-run the full store with bounded domain concurrency. LMDB has one writer per environment, so concurrency is only across independent domain environments:

```bash
java $JAVA_LMDB_FLAGS -cp "$CP" \
  org.texttechnologylab.duui.dua.uce.BIOfidAnnotationViewMigration \
  /storage/projects/BIOfid/code/dterefe/artifacts/dua-heideltime \
  --workers=4 | tee /tmp/biofid_annotation_view_migration_full_dryrun.log
```

Only apply after the final totals show `unresolvedSpans=0`:

```bash
java $JAVA_LMDB_FLAGS -cp "$CP" \
  org.texttechnologylab.duui.dua.uce.BIOfidAnnotationViewMigration \
  /storage/projects/BIOfid/code/dterefe/artifacts/dua-heideltime \
  --workers=4 --apply | tee /tmp/biofid_annotation_view_migration_apply.log
```

Migration implementation requirements:

```text
rewrite packed span blocks, not just direct span rows
update annotation FS view metadata to the document artifact initial view
rewrite AnnotationBase.sofa graph references in batches grouped by graph block
do not remove and add graph edges one annotation at a time
keep each domain migration transactional
checkpoint each migrated domain
print per-domain counts and final totals
```

Troubleshooting:

```text
all spans unresolved:
  check whether durable sofa refs are signed negative; reject only zero, not negative refs
  check whether registry document ids include a corpus/document prefix
  check whether the registry stores local sofa refs while spans store durable sofa refs

some spans unresolved:
  check allocation-order fallback within the same durable domain base
  map to nearest following document artifact first, then nearest preceding document artifact
  never map across a different high 32-bit domain base

apply is too slow:
  inspect CPU/RSS with ps
  if it is doing per-annotation graph rewrites, stop it and implement batched graph-block rewrites
  completed LMDB transactions remain committed; an interrupted active transaction is discarded

service still shows old view data:
  restart only dua-biofid-heideltime-service-17876 after apply
  do not restart the importer unless the user asks
```

After apply, verify with a full dry-run again. Correct shape after a successful apply is:

```text
alreadyInitialSpans=N
migratedSpans=0
unresolvedSpans=0
```

Then restart only the DUA service and check:

```bash
podman restart dua-biofid-heideltime-service-17876
curl -fsS http://127.0.0.1:17876/health
curl -sS http://127.0.0.1:17876/stats
```

## Final Verification Checklist

Run before reporting success:

```bash
podman ps --format '{{.Names}} {{.ID}} {{.Status}} {{.Ports}}' | rg 'dua-biofid-heideltime|uce-dua-web-live'
curl -fsS http://127.0.0.1:17876/health
curl -sS http://127.0.0.1:17876/stats
```

Verify:

```text
importer container is still Up
service container name is unchanged
HTTP dashboard loads at localhost:17876
WebSocket responds at localhost:17877
UCE DUA web proxy uses /ws/duaviz
document.list returns within timeout
stats has no top-level stale document-local error
```
