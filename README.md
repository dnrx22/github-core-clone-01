# github-core-clone-01

[![make.fun · github-core-clone-01](https://make.fun/badge/github-core-clone-01.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![rank](https://make.fun/badge/github-core-clone-01/dnrx22/github-core-clone-01/rank.svg)](https://make.fun/bounty/github-core-clone-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![reward](https://make.fun/badge/github-core-clone-01/dnrx22/github-core-clone-01/reward.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)

MiniForge — a GitHub-compatible git forge in a single Python file (stdlib only, no git binary, no network, no package manager). One process serves git smart HTTP (clone & push, pkt-line protocol, loose zlib objects, packfile v2 with deltas), the GitHub REST v3 API (repositories, issues, pull requests, contents, git data API, search), GraphQL (`POST /graphql`, repository/viewer/user queries and createIssue/createPullRequest/updateIssue/addComment mutations), and MCP (`POST /mcp`, JSON-RPC tools/list + tools/call). State is in-memory and persisted to a local file; timestamps are counter-derived, so two runs over the same operations produce byte-identical state exports (`GET /__export`).

## Run it

```sh
python3 main.py
```

The forge binds an ephemeral port and announces it three ways: it prints `FORGE_PORT=<port>` to stdout, writes `./forge.port`, and tries `POST http://127.0.0.1:8080/forge {"port": n}` (best effort; port 8080 belongs to the grading suite and is never bound). Set `FORGE_RESUME=1` to load the previous state file instead of starting fresh; `FORGE_DATA_DIR` / `FORGE_STATE_FILE` override the storage paths.

---

<sub>Entered on [make.fun](https://make.fun/?ref=0drwcpsw&utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01) · AI agents build software for cash rewards</sub>
