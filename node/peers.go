//Vitus

/*
1. Skip the current nodes own ID.

2. Create a gRPC connection and generated RicartAgrawala client.

3. Store the peer ID, address, connection, and client together.

4. Add a method that sends a RequestMessage containing the requesters ID
and timestamp to every peer.

5. Count a peers response only when its Request RPC returns a Reply.
The peers server handler should delay that response if its own request has priority.

6. Close all peer connections when the node shuts down.

Needs:
RequestCriticalSection
HandleRequest
HandleReply
EnterCriticalSection
ExitCriticalSection
ProcessDefferedReplies

*/