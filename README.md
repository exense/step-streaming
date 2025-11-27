# Step Streaming project

This is a standalone project providing the functionality to implement "streaming" up- and downloads.

It is primarily meant to be used within the Step product, but it could also be used standalone.

"Streaming" here has a slightly special meaning. The purpose is to allow downloads of resources that are
still being produced. We provide a reference implementation based on Websockets.

## Websockets implementation high-level description

1. Upload clients connect to a configurable server endpoint, send a "request upload" message with the file metadata, and
   expect a "ready for upload" message in return, containing a reference to the endpoint identifying the resource (i.e.,
   a URL/URI). Uploads are currently sent as a one-shot stream upload (i.e., they are not resumable).
2. On the server side, as soon as an upload is initiated, the corresponding resource will be available for download
   requests, i.e., download clients may connect to the endpoint indicated by the reference, and start requesting data.
3. To provide this functionality, ongoing uploads are checkpointed at regular (configurable) intervals, emitting status
   updates about the current size of the resource. These status updates are forwarded to all clients interested in the
   resource in question.
4. A download client connects to the given reference endpoint, and immediately receives the latest known status. It will
   also receive any potential status updates as soon as they happen on the server side.
5. Clients may at any time request any chunk of the data, identified by a start and end offset, as long as the end
   offset is within the limits of the last known/checkpointed size on the server.
6. A typical scenario for a client wishing to retrieve the full file while it is being produced is:
  - Wait for new status messages to arrive
  - Download the next chunk of data (start offset=offset of already received data, end offset=current file size)
  - Terminate once all data is received, as indicated by the status message (`status==COMPLETED`)
7. Of course, it is also possible to retrieve data that has been fully uploaded. In that case, only a single chunk
   download is actually performed.
8. This implementation has an additional benefit: clients may also request only parts of a file, for instance for
   client-side pagination of large log files.

## Project structure

There are currently 7 root-level modules: 4 for the API definitions (common, server, client-upload, client-download),
and 2 containing (potentially partial) implementation classes (impl-common, impl-server). The Websocket implementation (
impl-websocket) is provided as another module, with submodules mirroring the API modules.

Note that nontrivial Unit tests are consolidated in the websocket server module, because a full "infrastructure"
including client and server parts is required for meaningful tests.


