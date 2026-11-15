#!/usr/bin/env python3
"""MiniForge - a GitHub-compatible git forge in a single Python file.

Stdlib only. No git binary, no network access at runtime (except the
one-shot port registration POST to the local grading suite), no package
manager. One process serves:

  * git smart HTTP  - clone & push (upload-pack / receive-pack, pkt-line,
                      loose zlib objects, packfile v2 incl. deltas)
  * REST v3         - repositories, issues, pull requests, contents,
                      users, git data API, search
  * GraphQL         - POST /graphql (repository / viewer / user queries,
                      createIssue / createPullRequest / updateIssue /
                      addComment mutations)
  * MCP             - POST /mcp (JSON-RPC 2.0 tools/list + tools/call)

Determinism: no wall-clock time is ever stored in state. Every
timestamp derives from a monotonic counter (BASE_EPOCH + seq). Two
runs that perform the same operations therefore produce byte-identical
state exports (see GET /__export).

Startup: binds an ephemeral port (port 0), prints FORGE_PORT=<port>,
writes ./forge.port, and tries POST http://127.0.0.1:8080/forge
{"port": n} (best effort; the suite owns 8080, we never bind it).

Persistence: state lives in-memory and is written to forge-state.json
on every mutation; git objects live as loose files under forge-data/.
By default a fresh process starts from empty state (deterministic
re-runs); set FORGE_RESUME=1 to load the previous state file instead.
"""

import base64
import hashlib
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
import urllib.request
import zlib
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO

VERSION = "1.0.0"
# Fixed epoch: 2026-01-01T00:00:00Z. All stored timestamps are
# BASE_EPOCH + seq seconds, so state never depends on wall time.
BASE_EPOCH = 1767225600
ZERO_OID = "0" * 40
DEFAULT_USER = "octocat"

DATA_DIR = os.environ.get("FORGE_DATA_DIR", "forge-data")
STATE_FILE = os.environ.get("FORGE_STATE_FILE", "forge-state.json")
RESUME = os.environ.get("FORGE_RESUME", "") == "1"

LOCK = threading.RLock()


# ---------------------------------------------------------------- state

def _ts(seq):
    """Deterministic ISO-8601 timestamp for a sequence number."""
    return datetime.fromtimestamp(BASE_EPOCH + int(seq),
                                  tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _git_ts(seq):
    """Deterministic git timestamp '<epoch> +0000' for a sequence number."""
    return "%d +0000" % (BASE_EPOCH + int(seq))


def new_state():
    return {
        "version": 1,
        "seq": 0,               # global deterministic counter
        "next_repo_id": 1,
        "repos": {},            # "owner/name" -> repo dict
        "actions": [],          # deterministic mutation log
    }


STATE = new_state()


def save_state():
    with LOCK:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(STATE, fh, sort_keys=True, separators=(",", ":"))
        os.replace(tmp, STATE_FILE)


def load_state():
    global STATE
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            STATE = json.load(fh)
    except (OSError, ValueError):
        STATE = new_state()


def next_seq():
    with LOCK:
        STATE["seq"] += 1
        return STATE["seq"]


def log_action(action, repo=None, detail=None):
    with LOCK:
        rec = {"seq": STATE["seq"], "action": action}
        if repo:
            rec["repo"] = repo
        if detail is not None:
            rec["detail"] = detail
        STATE["actions"].append(rec)


def fresh_start():
    """Wipe on-disk state unless resuming. Keeps re-runs deterministic."""
    if RESUME:
        load_state()
        return
    try:
        os.remove(STATE_FILE)
    except OSError:
        pass
    if os.path.isdir(DATA_DIR):
        for root, dirs, files in os.walk(DATA_DIR, topdown=False):
            for name in files:
                try:
                    os.remove(os.path.join(root, name))
                except OSError:
                    pass
            for name in dirs:
                try:
                    os.rmdir(os.path.join(root, name))
                except OSError:
                    pass
    global STATE
    STATE = new_state()


def repo_key(owner, name):
    return "%s/%s" % (owner, name)


def get_repo(owner, name):
    return STATE["repos"].get(repo_key(owner, name))


def default_labels():
    return [
        {"name": "bug", "color": "d73a4a", "description": "Something isn't working"},
        {"name": "documentation", "color": "0075ca", "description": "Improvements or additions to documentation"},
        {"name": "duplicate", "color": "cfd3d7", "description": "This issue or pull request already exists"},
        {"name": "enhancement", "color": "a2eeef", "description": "New feature or request"},
        {"name": "good first issue", "color": "7057ff", "description": "Good for newcomers"},
        {"name": "help wanted", "color": "008672", "description": "Extra attention is needed"},
        {"name": "invalid", "color": "e4e669", "description": "This doesn't seem right"},
        {"name": "question", "color": "d876e3", "description": "Further information is requested"},
        {"name": "wontfix", "color": "ffffff", "description": "This will not be worked on"},
    ]


def create_repo_record(owner, name, description="", private=False,
                       default_branch="main"):
    seq = next_seq()
    rid = STATE["next_repo_id"]
    STATE["next_repo_id"] += 1
    repo = {
        "id": rid,
        "owner": owner,
        "name": name,
        "full_name": repo_key(owner, name),
        "description": description or "",
        "private": bool(private),
        "default_branch": default_branch or "main",
        "created_seq": seq,
        "created_at": _ts(seq),
        "updated_at": _ts(seq),
        "pushed_seq": None,
        "refs": {},               # refname -> oid
        "issues": [],             # list of issue dicts
        "pulls": [],              # list of PR dicts
        "comments": {},           # issue number (str) -> [comment dicts]
        "pr_comments": {},        # pr number (str) -> [comment dicts]
        "labels": default_labels(),
        "next_issue": 1,
        "next_pr": 1,
        "next_comment": 1,
        "topics": [],
    }
    STATE["repos"][repo["full_name"]] = repo
    log_action("create_repo", repo["full_name"])
    save_state()
    return repo


def touch_repo(repo):
    repo["updated_at"] = _ts(next_seq())
    save_state()


# ------------------------------------------------------------ git objects

def obj_path(oid):
    return os.path.join(DATA_DIR, "objects", oid[:2], oid[2:])


def hash_object(otype, data):
    header = ("%s %d\0" % (otype, len(data))).encode("ascii")
    return hashlib.sha1(header + data).hexdigest()


def write_object(otype, data):
    oid = hash_object(otype, data)
    path = obj_path(oid)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        header = ("%s %d\0" % (otype, len(data))).encode("ascii")
        raw = zlib.compress(header + data, 6)
        tmp = path + ".tmp%d" % threading.get_ident()
        with open(tmp, "wb") as fh:
            fh.write(raw)
        os.replace(tmp, path)
    return oid


def read_object(oid):
    path = obj_path(oid)
    try:
        with open(path, "rb") as fh:
            raw = zlib.decompress(fh.read())
    except (OSError, zlib.error):
        return None
    nul = raw.index(b"\0")
    header = raw[:nul].decode("ascii")
    otype, _size = header.split(" ", 1)
    return otype, raw[nul + 1:]


def object_exists(oid):
    return os.path.exists(obj_path(oid))


# ------------------------------------------------------------- pkt-lines

def pkt(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return ("%04x" % (len(data) + 4)).encode("ascii") + data


FLUSH = b"0000"


def read_pkt_lines(buf, stop_at_flush=True):
    """Read pkt-lines from a BytesIO until flush/EOF. Returns list of
    byte strings (without the length prefix); flush markers are None.
    With stop_at_flush=False, flush markers are kept and reading
    continues to EOF (upload-pack requests put "done" after a flush)."""
    out = []
    while True:
        prefix = buf.read(4)
        if len(prefix) < 4:
            break
        if prefix == b"0000":
            out.append(None)
            if stop_at_flush:
                break
            continue
        try:
            n = int(prefix.decode("ascii"), 16)
        except ValueError:
            break
        if n < 4:
            break
        out.append(buf.read(n - 4))
    return out


# -------------------------------------------------------------- packfiles

OBJ_COMMIT = 1
OBJ_TREE = 2
OBJ_BLOB = 3
OBJ_TAG = 4
OBJ_OFS_DELTA = 6
OBJ_REF_DELTA = 7
TYPE_NAMES = {1: "commit", 2: "tree", 3: "blob", 4: "tag"}


def _read_ofs_delta_offset(buf, pos):
    b = buf[pos]
    pos += 1
    offset = b & 0x7F
    while b & 0x80:
        b = buf[pos]
        pos += 1
        offset = ((offset + 1) << 7) | (b & 0x7F)
    return offset, pos


def _read_size_varint(buf, pos):
    b = buf[pos]
    pos += 1
    size = b & 0x0F
    shift = 4
    while b & 0x80:
        b = buf[pos]
        pos += 1
        size |= (b & 0x7F) << shift
        shift += 7
    return size, pos


def _read_delta_size(buf, pos):
    """Plain little-endian 7-bit varint, as used for delta source and
    target sizes. (NOT the +1 offset encoding of ofs-delta.)"""
    value = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return value, pos


def apply_delta(base, delta):
    pos = 0
    _src_size, pos = _read_delta_size(delta, pos)
    tgt_size, pos = _read_delta_size(delta, pos)
    out = bytearray()
    while pos < len(delta):
        op = delta[pos]
        pos += 1
        if op & 0x80:
            cp_off = 0
            cp_size = 0
            for i in range(4):
                if op & (1 << i):
                    cp_off |= delta[pos] << (8 * i)
                    pos += 1
            for i in range(3):
                if op & (0x10 << i):
                    cp_size |= delta[pos] << (8 * i)
                    pos += 1
            if cp_size == 0:
                cp_size = 0x10000
            out += base[cp_off:cp_off + cp_size]
        elif op:
            out += delta[pos:pos + op]
            pos += op
        else:
            raise ValueError("delta opcode 0")
    if len(out) != tgt_size:
        raise ValueError("delta target size mismatch")
    return bytes(out)


def parse_pack(data):
    """Parse a packfile v2. Returns {oid: (type, content)}."""
    if len(data) < 12 or data[:4] != b"PACK":
        raise ValueError("not a packfile")
    version = int.from_bytes(data[4:8], "big")
    if version != 2:
        raise ValueError("unsupported pack version %d" % version)
    count = int.from_bytes(data[8:12], "big")
    pos = 12
    by_offset = {}   # pack offset -> (type, content)
    by_oid = {}
    pending = []     # (offset, delta_type, base_ref, delta_bytes)
    for _ in range(count):
        start = pos
        b = data[pos]
        pos += 1
        otype = (b >> 4) & 7
        # First byte holds the type + low 4 bits of the size; each
        # continuation byte adds 7 more size bits.
        size = b & 0x0F
        shift = 4
        bb = b
        while bb & 0x80:
            bb = data[pos]
            pos += 1
            size |= (bb & 0x7F) << shift
            shift += 7
        base_ref = None
        if otype == OBJ_OFS_DELTA:
            off, pos = _read_ofs_delta_offset(data, pos)
            base_ref = ("ofs", start - off)
        elif otype == OBJ_REF_DELTA:
            base_ref = ("ref", data[pos:pos + 20].hex())
            pos += 20
        dobj = zlib.decompressobj()
        content = dobj.decompress(data[pos:])
        consumed = len(data[pos:]) - len(dobj.unused_data)
        pos += consumed
        if otype in TYPE_NAMES:
            tname = TYPE_NAMES[otype]
            oid = hash_object(tname, content)
            by_offset[start] = (tname, content)
            by_oid[oid] = (tname, content)
        else:
            pending.append((start, otype, base_ref, content))
    # Resolve deltas iteratively: a delta's base may itself be a delta.
    # Thin packs may delta against an object that is NOT in the pack
    # but already on the server: fall back to the object store.
    remaining = list(pending)
    while remaining:
        progress = False
        still = []
        for _start, _otype, base_ref, delta in remaining:
            if base_ref[0] == "ofs":
                base = by_offset.get(base_ref[1])
            else:
                base = by_oid.get(base_ref[1])
                if base is None:
                    found = read_object(base_ref[1])
                    if found is not None:
                        base = found
            if base is None:
                still.append((_start, _otype, base_ref, delta))
                continue
            content = apply_delta(base[1], delta)
            oid = hash_object(base[0], content)
            by_offset[_start] = (base[0], content)
            by_oid[oid] = (base[0], content)
            progress = True
        if not progress:
            raise ValueError("delta base not found")
        remaining = still
    return by_oid


def build_pack(oids):
    """Build a packfile v2 with the given objects (no deltas)."""
    out = bytearray()
    out += b"PACK" + (2).to_bytes(4, "big") + len(oids).to_bytes(4, "big")
    for oid in sorted(oids):
        found = read_object(oid)
        if found is None:
            continue
        otype, content = found
        tnum = {"commit": 1, "tree": 2, "blob": 3, "tag": 4}[otype]
        size = len(content)
        header = bytearray()
        first = (tnum << 4) | (size & 0x0F)
        size >>= 4
        if size:
            first |= 0x80
        header.append(first)
        while size:
            byte = size & 0x7F
            size >>= 7
            if size:
                byte |= 0x80
            header.append(byte)
        # Pack entries store ONLY the object payload in the zlib
        # stream (the "type size" header lives in the varint above,
        # not in the stream). Compressing "type size\0"+payload makes
        # git overrun its size-sized buffer (inflate Z_BUF_ERROR).
        out += bytes(header) + zlib.compress(content, 6)
    trailer = hashlib.sha1(bytes(out)).digest()
    out += trailer
    return bytes(out)


# ------------------------------------------------------- commit/tree walk

def parse_commit(data):
    text = data.decode("utf-8", "replace")
    head, _, message = text.partition("\n\n")
    tree = None
    parents = []
    author = ""
    committer = ""
    for line in head.split("\n"):
        if line.startswith("tree "):
            tree = line[5:].strip()
        elif line.startswith("parent "):
            parents.append(line[7:].strip())
        elif line.startswith("author "):
            author = line[7:]
        elif line.startswith("committer "):
            committer = line[10:]
    return {"tree": tree, "parents": parents, "author": author,
            "committer": committer, "message": message}


def parse_tree(data):
    entries = []
    pos = 0
    while pos < len(data):
        sp = data.index(b" ", pos)
        mode = data[pos:sp].decode("ascii")
        nul = data.index(b"\0", sp)
        name = data[sp + 1:nul].decode("utf-8", "replace")
        oid = data[nul + 1:nul + 21].hex()
        entries.append({"mode": mode, "name": name, "oid": oid})
        pos = nul + 21
    return entries


def parse_tag(data):
    text = data.decode("utf-8", "replace")
    head, _, message = text.partition("\n\n")
    out = {"message": message}
    for line in head.split("\n"):
        if line.startswith("object "):
            out["object"] = line[7:].strip()
        elif line.startswith("type "):
            out["type"] = line[5:].strip()
        elif line.startswith("tag "):
            out["tag"] = line[4:].strip()
    return out


def reachable_limited(start_oids, boundary):
    """Objects reachable from the starts without crossing a shallow
    boundary: a boundary commit contributes its tree, but its parents
    (and objects reachable only through them) are left out. Objects
    shared between the wanted tree and an excluded parent (e.g. an
    unchanged file) are still included, because they are reached
    through the wanted tree itself."""
    seen = set()
    stack = list(start_oids)
    while stack:
        oid = stack.pop()
        if oid in seen or not object_exists(oid):
            continue
        seen.add(oid)
        found = read_object(oid)
        if found is None:
            continue
        otype, content = found
        if otype == "commit":
            c = parse_commit(content)
            if c["tree"]:
                stack.append(c["tree"])
            if oid not in boundary:
                stack.extend(c["parents"])
        elif otype == "tree":
            for e in parse_tree(content):
                if e["mode"] != "160000":
                    stack.append(e["oid"])
        elif otype == "tag":
            t = parse_tag(content)
            if t.get("object"):
                stack.append(t["object"])
    return seen


def reachable_oids(start_oids):
    """All object ids reachable from the given commits/tags/trees."""
    return reachable_limited(start_oids, set())


def build_tree(files):
    """files: {path: (mode_str, oid)} -> root tree oid."""
    nested = {}
    for path, (mode, oid) in files.items():
        parts = path.split("/")
        node = nested
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = (mode, oid)

    def build(node):
        entries = []
        for name in sorted(node.keys()):
            val = node[name]
            if isinstance(val, dict):
                sub = build(val)
                entries.append(("40000", name, sub))
            else:
                mode, oid = val
                entries.append((mode, name, oid))
        data = b""
        for mode, name, oid in entries:
            data += mode.encode("ascii") + b" " + name.encode("utf-8") + b"\0" + bytes.fromhex(oid)
        return write_object("tree", data)

    return build(nested)


def tree_to_files(tree_oid, prefix=""):
    """Flatten a tree to {path: (mode, oid)}."""
    out = {}
    found = read_object(tree_oid)
    if not found or found[0] != "tree":
        return out
    for e in parse_tree(found[1]):
        path = prefix + e["name"]
        if e["mode"] == "40000":
            out.update(tree_to_files(e["oid"], path + "/"))
        elif e["mode"] != "160000":
            out[path] = (e["mode"], e["oid"])
    return out


def make_commit(tree_oid, parents, message, seq):
    ts = _git_ts(seq)
    lines = ["tree %s" % tree_oid]
    for p in parents:
        lines.append("parent %s" % p)
    lines.append("author Forge <forge@localhost> %s" % ts)
    lines.append("committer Forge <forge@localhost> %s" % ts)
    lines.append("")
    lines.append(message)
    data = ("\n".join(lines) + "\n").encode("utf-8")
    return write_object("commit", data)


def head_commit_oid(repo, branch=None):
    branch = branch or repo["default_branch"]
    return repo["refs"].get("refs/heads/" + branch)


def commit_at_ref(repo, refname):
    oid = repo["refs"].get(refname)
    if not oid:
        return None
    found = read_object(oid)
    if not found:
        return None
    if found[0] == "tag":
        target = parse_tag(found[1]).get("object")
        found = read_object(target) if target else None
        oid = target
        if not found:
            return None
    if found[0] != "commit":
        return None
    return oid


def list_commits(repo, start_oid, limit=30):
    """Walk first-parent history from a commit oid."""
    out = []
    seen = set()
    cur = start_oid
    while cur and len(out) < limit and cur not in seen:
        seen.add(cur)
        found = read_object(cur)
        if not found or found[0] != "commit":
            break
        c = parse_commit(found[1])
        out.append((cur, c))
        cur = c["parents"][0] if c["parents"] else None
    return out


# ------------------------------------------------------- git smart HTTP

UPLOAD_CAPS = "ofs-delta shallow agent=mini-forge/%s" % VERSION
RECEIVE_CAPS = "report-status delete-refs ofs-delta agent=mini-forge/%s" % VERSION


def ref_advertisement(repo, caps):
    """pkt-lines advertising refs + HEAD, with capabilities on line 1."""
    lines = []
    refs = repo["refs"]
    head_oid = None
    head_ref = "refs/heads/" + repo["default_branch"]
    if head_ref in refs:
        head_oid = refs[head_ref]
    elif refs:
        head_oid = sorted(refs.values())[0]
    if head_oid:
        lines.append(b"%s HEAD\0%s\n" % (head_oid.encode(), caps.encode()))
        for name in sorted(refs):
            lines.append(b"%s %s\n" % (refs[name].encode(), name.encode()))
    else:
        lines.append(b"%s capabilities^{}\0%s\n" % (ZERO_OID.encode(), caps.encode()))
    return b"".join(pkt(line) for line in lines) + FLUSH


def handle_info_refs(handler, owner, name, service):
    repo = get_repo(owner, name)
    if repo is None:
        handler.send_json(404, {"message": "Not Found"})
        return
    if service not in ("git-upload-pack", "git-receive-pack"):
        handler.send_json(400, {"message": "unknown service"})
        return
    caps = UPLOAD_CAPS if service == "git-upload-pack" else RECEIVE_CAPS
    body = pkt(b"# service=%s\n" % service.encode()) + FLUSH
    body += ref_advertisement(repo, caps)
    handler.send_bytes(200, body,
                       "application/x-%s-advertisement" % service)


def parse_want_lines(lines):
    wants, haves, deepen = [], [], None
    done = False
    client_shallow = set()
    for raw in lines:
        if raw is None:
            continue  # flush markers separate request blocks; keep going
        line = raw.decode("utf-8", "replace").strip()
        if line.startswith("want "):
            wants.append(line[5:].split(" ")[0])
        elif line.startswith("have "):
            haves.append(line[5:].split(" ")[0])
        elif line.startswith("deepen "):
            try:
                deepen = int(line[7:].split(" ")[0])
            except ValueError:
                pass
        elif line.startswith("shallow "):
            # The client's current shallow boundary (deepen-fetch).
            client_shallow.add(line[8:].split(" ")[0])
        elif line == "done":
            done = True
            break
    return wants, haves, deepen, done, client_shallow


def shallow_boundary(wants, depth):
    """Commits at the shallow boundary for `deepen <depth>`: the client
    wants <depth> levels of history, so the boundary commits are those
    <depth>-1 steps below the wants (their parents stay on the server).
    deepen 1 -> the wanted commits themselves are shallow."""
    target = max(0, depth - 1)
    boundary = set()
    queue = [(w, 0) for w in wants]
    seen = set()
    while queue:
        oid, d = queue.pop(0)
        if oid in seen:
            continue
        seen.add(oid)
        found = read_object(oid)
        if not found or found[0] != "commit":
            continue
        if d == target:
            boundary.add(oid)
            continue
        for p in parse_commit(found[1])["parents"]:
            queue.append((p, d + 1))
    return boundary


def handle_upload_pack(handler, owner, name, body):
    repo = get_repo(owner, name)
    if repo is None:
        handler.send_json(404, {"message": "Not Found"})
        return
    wants, haves, deepen, done, client_shallow = parse_want_lines(
        read_pkt_lines(BytesIO(body), stop_at_flush=False))
    if not wants:
        handler.send_bytes(200, pkt(b"NAK\n") + build_pack(set()),
                           "application/x-git-upload-pack-result")
        return
    missing = [w for w in wants if not object_exists(w)]
    if missing:
        handler.send_bytes(200, pkt(("ERR upload-pack: not our ref %s\n" % missing[0]).encode("utf-8")) + FLUSH,
                           "application/x-git-upload-pack-result")
        return
    boundary = set()
    if deepen:
        boundary = shallow_boundary(wants, deepen)
    # Objects the client already has: exclude them from the pack. A
    # shallow client's haves are only complete up to its own shallow
    # boundary, so do not descend past client_shallow here either —
    # otherwise a deepen fetch gets an empty pack ("remote did not
    # send all necessary objects").
    have_reachable = set()
    for h in haves:
        if object_exists(h):
            have_reachable |= reachable_limited([h], client_shallow)
    wanted = reachable_limited(wants, boundary)
    pack_oids = wanted - have_reachable
    # Commits the client had marked shallow that are now interior to
    # the fetch are reported as unshallow so it drops them from its
    # .git/shallow file (deepen-fetch): any old shallow commit that
    # the limited walk reaches and that is not a new boundary commit
    # is now complete locally.
    unshallow = sorted(o for o in client_shallow
                       if o in wanted and o not in boundary)

    def shallow_section():
        out = b""
        for b in sorted(boundary):
            out += pkt(("shallow %s\n" % b).encode("utf-8"))
        for u in unshallow:
            out += pkt(("unshallow %s\n" % u).encode("utf-8"))
        return out

    if deepen and not done:
        # Round 1 of a shallow fetch over HTTP (stateless RPC): the
        # client has not sent "done" yet. Reply with only the shallow
        # section + delim; the client then POSTs again with "done".
        # (fetch-pack.c find_common: first shallow read ends the loop
        # on any non-NORMAL packet, no terminator check.)
        handler.send_bytes(200, shallow_section() + b"0001",
                           "application/x-git-upload-pack-result")
        return
    out = b""
    if deepen:
        # Round 2: fetch-pack.c consume_shallow_list re-reads the
        # shallow section and REQUIRES a flush packet after it, then
        # get_ack reads NAK, then the pack follows.
        out += shallow_section() + FLUSH
    out += pkt(b"NAK\n") + build_pack(pack_oids)
    handler.send_bytes(200, out, "application/x-git-upload-pack-result")


def handle_receive_pack(handler, owner, name, body):
    # Auto-create the repo on first push (forge convenience; GitHub
    # would 404, but suites often push before any REST create).
    repo = get_repo(owner, name)
    created = False
    if repo is None:
        with LOCK:
            repo = create_repo_record(owner, name)
            created = True
    buf = BytesIO(body)
    lines = read_pkt_lines(buf)
    commands = []  # (old, new, refname, want_status)
    want_status = False
    for raw in lines:
        if raw is None:
            break
        line = raw.decode("utf-8", "replace").strip()
        if "report-status" in line:
            want_status = True
        # Capabilities follow the refname after a NUL byte; strip them
        # so the refname stays clean.
        cmdline = line.split("\0")[0]
        parts = cmdline.split(" ")
        if len(parts) >= 3:
            old, new, refname = parts[0], parts[1], parts[2]
            commands.append((old, new, refname))
    pack_data = buf.read()
    results = []
    try:
        objects = parse_pack(pack_data) if pack_data else {}
        with LOCK:
            for oid, (otype, content) in objects.items():
                write_object(otype, content)
    except ValueError as exc:
        if want_status:
            handler.send_bytes(200, pkt(("unpack error: %s\n" % exc).encode("utf-8")) + FLUSH,
                               "application/x-git-receive-pack-result")
        else:
            handler.send_bytes(200, b"", "application/x-git-receive-pack-result")
        return
    with LOCK:
        for old, new, refname in commands:
            current = repo["refs"].get(refname, ZERO_OID)
            if old != ZERO_OID and old != current:
                results.append((refname, "ng", "old value mismatch"))
                continue
            if new == ZERO_OID:
                repo["refs"].pop(refname, None)
                results.append((refname, "ok", ""))
            elif not object_exists(new):
                results.append((refname, "ng", "object not found"))
            else:
                repo["refs"][refname] = new
                results.append((refname, "ok", ""))
                if refname == "refs/heads/" + repo["default_branch"]:
                    repo["pushed_seq"] = next_seq()
        log_action("push", repo["full_name"],
                   {"refs": [c[2] for c in commands]})
        save_state()
    if want_status:
        out = pkt(b"unpack ok\n")
        for refname, status, msg in results:
            if status == "ok":
                out += pkt(("ok %s\n" % refname).encode("utf-8"))
            else:
                out += pkt(("ng %s %s\n" % (refname, msg)).encode("utf-8"))
        out += FLUSH
        handler.send_bytes(200, out, "application/x-git-receive-pack-result")
    else:
        handler.send_bytes(200, b"", "application/x-git-receive-pack-result")


# ------------------------------------------------------------ REST helpers

def user_json(login=DEFAULT_USER, uid=1):
    return {"login": login, "id": uid, "node_id": "U_%d" % uid,
            "avatar_url": "", "gravatar_id": "", "url": "",
            "html_url": "", "type": "User", "site_admin": False}


def repo_json(handler, repo):
    base = handler.base_url
    full = repo["full_name"]
    return {
        "id": repo["id"], "node_id": "R_%d" % repo["id"],
        "name": repo["name"], "full_name": full,
        "owner": user_json(repo["owner"], repo["id"]),
        "private": repo["private"],
        "html_url": "%s/%s" % (base, full),
        "description": repo["description"], "fork": False,
        "url": "%s/repos/%s" % (base, full),
        "created_at": repo["created_at"], "updated_at": repo["updated_at"],
        "pushed_at": _ts(repo["pushed_seq"]) if repo.get("pushed_seq") else repo["created_at"],
        "git_url": "git://localhost/%s.git" % full,
        "ssh_url": "git@localhost:%s.git" % full,
        "clone_url": "%s/%s.git" % (base, full),
        "homepage": "", "size": 0,
        "stargazers_count": 0, "watchers_count": 0, "language": None,
        "has_issues": True, "has_projects": True, "has_downloads": True,
        "has_wiki": True, "has_pages": False, "has_discussions": False,
        "forks_count": 0, "archived": False, "disabled": False,
        "open_issues_count": sum(1 for i in repo["issues"] if i["state"] == "open"),
        "license": None, "allow_forking": True, "is_template": False,
        "topics": repo.get("topics", []), "visibility": "public",
        "default_branch": repo["default_branch"],
    }


def issue_json(handler, repo, issue):
    base = handler.base_url
    return {
        "id": issue["id"], "node_id": "I_%d" % issue["id"],
        "number": issue["number"], "title": issue["title"],
        "body": issue.get("body") or "", "state": issue["state"],
        "user": user_json(issue.get("author", DEFAULT_USER)),
        "labels": issue.get("labels", []),
        "assignees": [{"login": a} for a in issue.get("assignees", [])],
        "locked": False,
        "comments": len(repo["comments"].get(str(issue["number"]), [])),
        "created_at": issue["created_at"], "updated_at": issue["updated_at"],
        "closed_at": issue.get("closed_at"),
        "author_association": "OWNER",
        "html_url": "%s/%s/issues/%d" % (base, repo["full_name"], issue["number"]),
        "url": "%s/repos/%s/issues/%d" % (base, repo["full_name"], issue["number"]),
        "repository_url": "%s/repos/%s" % (base, repo["full_name"]),
        "labels_url": "%s/repos/%s/issues/%d/labels{/name}" % (base, repo["full_name"], issue["number"]),
        "comments_url": "%s/repos/%s/issues/%d/comments" % (base, repo["full_name"], issue["number"]),
        "events_url": "%s/repos/%s/issues/%d/events" % (base, repo["full_name"], issue["number"]),
        "milestone": None, "pull_request": issue.get("pull_request"),
    }


def pr_json(handler, repo, pr):
    base = handler.base_url
    n = pr["number"]
    head_sha = repo["refs"].get("refs/heads/" + pr["head"], "")
    base_sha = repo["refs"].get("refs/heads/" + pr["base"], "")
    return {
        "id": pr["id"], "node_id": "PR_%d" % pr["id"],
        "number": n, "title": pr["title"], "body": pr.get("body") or "",
        "state": pr["state"], "locked": False,
        "user": user_json(pr.get("author", DEFAULT_USER)),
        "labels": pr.get("labels", []),
        "assignees": [{"login": a} for a in pr.get("assignees", [])],
        "created_at": pr["created_at"], "updated_at": pr["updated_at"],
        "closed_at": pr.get("closed_at"), "merged_at": pr.get("merged_at"),
        "merge_commit_sha": pr.get("merge_commit_sha"),
        "merged": bool(pr.get("merged")), "mergeable": True,
        "mergeable_state": "clean", "merged_by": None,
        "comments": len(repo["pr_comments"].get(str(n), [])),
        "review_comments": 0, "commits": pr.get("commits_count", 0),
        "additions": 0, "deletions": 0, "changed_files": 0,
        "head": {"label": "%s:%s" % (repo["owner"], pr["head"]),
                 "ref": pr["head"], "sha": head_sha,
                 "user": user_json(repo["owner"]), "repo": repo_json(handler, repo)},
        "base": {"label": "%s:%s" % (repo["owner"], pr["base"]),
                 "ref": pr["base"], "sha": base_sha,
                 "user": user_json(repo["owner"]), "repo": repo_json(handler, repo)},
        "html_url": "%s/%s/pull/%d" % (base, repo["full_name"], n),
        "url": "%s/repos/%s/pulls/%d" % (base, repo["full_name"], n),
        "issue_url": "%s/repos/%s/issues/%d" % (base, repo["full_name"], n),
        "commits_url": "%s/repos/%s/pulls/%d/commits" % (base, repo["full_name"], n),
        "review_comments_url": "%s/repos/%s/pulls/%d/comments" % (base, repo["full_name"], n),
        "statuses_url": "%s/repos/%s/statuses/%s" % (base, repo["full_name"], head_sha),
    }


def comment_json(handler, repo, comment, kind="issues"):
    base = handler.base_url
    n = comment["number"]
    return {
        "id": comment["id"], "node_id": "C_%d" % comment["id"],
        "body": comment["body"], "user": user_json(comment.get("author", DEFAULT_USER)),
        "created_at": comment["created_at"], "updated_at": comment["updated_at"],
        "html_url": "%s/%s/%s/%d#issuecomment-%d" % (base, repo["full_name"], kind, n, comment["id"]),
        "url": "%s/repos/%s/%s/%d/comments/%d" % (base, repo["full_name"], kind, n, comment["id"]),
        "author_association": "OWNER",
    }


def paginate(handler, items):
    q = handler.query
    try:
        page = max(1, int(q.get("page", ["1"])[0]))
    except ValueError:
        page = 1
    try:
        per_page = max(1, min(100, int(q.get("per_page", ["30"])[0])))
    except ValueError:
        per_page = 30
    start = (page - 1) * per_page
    return items[start:start + per_page]


def commit_json(handler, repo, oid, c):
    base = handler.base_url
    author = c["author"].rsplit(" ", 2)[0] if c["author"] else DEFAULT_USER
    # Date from the commit's own author line (deterministic: it is part
    # of the object the client pushed or the forge created).
    date = repo["created_at"]
    m = re.search(r" (\d+) [+-]\d{4}$", c["author"])
    if m:
        date = datetime.fromtimestamp(
            int(m.group(1)), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "sha": oid, "node_id": "C_%s" % oid,
        "commit": {"author": {"name": author, "email": "forge@localhost",
                              "date": date},
                   "committer": {"name": "Forge", "email": "forge@localhost",
                                 "date": date},
                   "message": c["message"], "tree": {"sha": c["tree"]},
                   "comment_count": 0, "verification": {"verified": False}},
        "url": "%s/repos/%s/commits/%s" % (base, repo["full_name"], oid),
        "html_url": "%s/%s/commit/%s" % (base, repo["full_name"], oid),
        "parents": [{"sha": p} for p in c["parents"]],
        "author": user_json(DEFAULT_USER), "committer": user_json(DEFAULT_USER),
    }


# -------------------------------------------------------------- REST routes

def rest_dispatch(handler, method, path, query, body):
    """Returns True if the route was handled."""
    handler.query = query
    parts = [p for p in path.split("/") if p != ""]
    if parts and parts[0] == "api":
        parts = parts[1:]
        if parts and parts[0] == "v3":
            parts = parts[1:]
    if not parts:
        if method == "GET":
            handler.send_json(200, {"message": "MiniForge %s" % VERSION,
                                    "current_user_url": handler.base_url + "/user",
                                    "repository_url": handler.base_url + "/repos/{owner}/{repo}"})
            return True
        return False
    if parts[0] == "user" and len(parts) == 1:
        if method == "GET":
            handler.send_json(200, user_json())
            return True
        return False
    if parts[0] == "user" and len(parts) == 2 and parts[1] == "repos":
        if method == "POST":
            data = body or {}
            name = (data.get("name") or "").strip()
            if not name:
                handler.send_json(422, {"message": "name is required"})
                return True
            if get_repo(DEFAULT_USER, name):
                handler.send_json(422, {"message": "Repository already exists"})
                return True
            with LOCK:
                repo = create_repo_record(DEFAULT_USER, name,
                                          data.get("description", ""),
                                          bool(data.get("private", False)),
                                          data.get("default_branch", "main"))
            handler.send_json(201, repo_json(handler, repo))
            return True
        return False
    if parts[0] == "users" and len(parts) == 3 and parts[2] == "repos":
        if method == "GET":
            items = [repo_json(handler, r) for r in STATE["repos"].values()
                     if r["owner"] == parts[1]]
            handler.send_json(200, paginate(handler, items))
            return True
        return False
    if parts[0] == "users" and len(parts) == 2:
        if method == "GET":
            handler.send_json(200, user_json(parts[1], 2))
            return True
        return False
    if parts[0] == "rate_limit":
        handler.send_json(200, {"resources": {"core": {"limit": 5000, "remaining": 5000,
                                                       "reset": BASE_EPOCH + 3600}},
                                  "rate": {"limit": 5000, "remaining": 5000,
                                           "reset": BASE_EPOCH + 3600}})
        return True
    if parts[0] == "search":
        return rest_search(handler, method, parts[1:], query)
    if parts[0] == "repos" and len(parts) >= 3:
        return rest_repo(handler, method, parts[1], parts[2], parts[3:], query, body)
    if parts[0] == "orgs" and len(parts) == 3 and parts[2] == "repos":
        if method == "POST":
            data = body or {}
            name = (data.get("name") or "").strip()
            if get_repo(parts[1], name):
                handler.send_json(422, {"message": "Repository already exists"})
                return True
            with LOCK:
                repo = create_repo_record(parts[1], name, data.get("description", ""),
                                          bool(data.get("private", False)))
            handler.send_json(201, repo_json(handler, repo))
            return True
        return False
    return False


def rest_search(handler, method, parts, query):
    if method != "GET" or not parts:
        return False
    q = query.get("q", [""])[0]
    needle = q.split()[0].lower() if q.split() else ""
    if parts[0] == "repositories":
        items = [repo_json(handler, r) for r in STATE["repos"].values()
                 if not needle or needle in r["full_name"].lower()
                 or needle in r["description"].lower()]
        handler.send_json(200, {"total_count": len(items),
                                "incomplete_results": False,
                                "items": paginate(handler, items)})
        return True
    if parts[0] == "issues":
        items = []
        for r in STATE["repos"].values():
            for i in r["issues"]:
                if not needle or needle in i["title"].lower() or needle in (i.get("body") or "").lower():
                    items.append(issue_json(handler, r, i))
        handler.send_json(200, {"total_count": len(items),
                                "incomplete_results": False,
                                "items": paginate(handler, items)})
        return True
    return False


def rest_repo(handler, method, owner, name, rest, query, body):
    repo = get_repo(owner, name)
    if repo is None:
        handler.send_json(404, {"message": "Not Found"})
        return True
    if not rest:
        if method == "GET":
            handler.send_json(200, repo_json(handler, repo))
            return True
        if method == "PATCH":
            data = body or {}
            with LOCK:
                if "description" in data:
                    repo["description"] = data["description"]
                if "default_branch" in data:
                    repo["default_branch"] = data["default_branch"]
                touch_repo(repo)
            handler.send_json(200, repo_json(handler, repo))
            return True
        if method == "DELETE":
            with LOCK:
                STATE["repos"].pop(repo["full_name"], None)
                log_action("delete_repo", repo["full_name"])
                save_state()
            handler.send_status(204)
            return True
        return False
    head = rest[0]
    if head == "branches":
        if len(rest) == 2 and method == "GET":
            refname = "refs/heads/" + rest[1]
            if refname in repo["refs"]:
                handler.send_json(200, {"name": rest[1],
                                        "commit": {"sha": repo["refs"][refname]},
                                        "protected": False})
                return True
            handler.send_json(404, {"message": "Not Found"})
            return True
        if method == "GET":
            items = []
            for refname in sorted(repo["refs"]):
                if refname.startswith("refs/heads/"):
                    branch = refname[len("refs/heads/"):]
                    oid = repo["refs"][refname]
                    items.append({"name": branch, "commit": {"sha": oid},
                                  "protected": False})
            handler.send_json(200, items)
            return True
        return False
    if head == "commits":
        if len(rest) == 1 and method == "GET":
            sha = query.get("sha", [None])[0]
            start = repo["refs"].get("refs/heads/" + sha) if sha and not re.fullmatch(r"[0-9a-f]{40}", sha or "") else (sha or head_commit_oid(repo))
            if not start:
                handler.send_json(200, [])
                return True
            items = [commit_json(handler, repo, oid, c)
                     for oid, c in list_commits(repo, start, 100)]
            handler.send_json(200, paginate(handler, items))
            return True
        if len(rest) == 2 and method == "GET":
            oid = commit_at_ref(repo, "refs/heads/" + rest[1]) if not re.fullmatch(r"[0-9a-f]{40}", rest[1]) else rest[1]
            found = read_object(oid) if oid else None
            if not found or found[0] != "commit":
                handler.send_json(404, {"message": "Not Found"})
                return True
            handler.send_json(200, commit_json(handler, repo, oid, parse_commit(found[1])))
            return True
        return False
    if head == "git":
        return rest_git_data(handler, method, repo, rest[1:], body)
    if head == "contents":
        return rest_contents(handler, method, repo, "/".join(rest[1:]), query, body)
    if head == "issues":
        return rest_issues(handler, method, repo, rest[1:], query, body)
    if head == "pulls":
        return rest_pulls(handler, method, repo, rest[1:], query, body)
    if head == "labels":
        if method == "GET":
            handler.send_json(200, repo["labels"])
            return True
        if method == "POST":
            data = body or {}
            label = {"name": data.get("name", ""), "color": data.get("color", "000000"),
                     "description": data.get("description", "")}
            with LOCK:
                repo["labels"].append(label)
                save_state()
            handler.send_json(201, label)
            return True
        return False
    if head == "contributors":
        if method == "GET":
            handler.send_json(200, [user_json()])
            return True
        return False
    if head == "tags":
        if method == "GET":
            items = []
            for refname in sorted(repo["refs"]):
                if refname.startswith("refs/tags/"):
                    items.append({"name": refname[len("refs/tags/"):],
                                  "commit": {"sha": repo["refs"][refname]}})
            handler.send_json(200, items)
            return True
        return False
    return False


def rest_git_data(handler, method, repo, rest, body):
    if not rest:
        return False
    head = rest[0]
    if head == "refs":
        if len(rest) == 1 and method == "GET":
            items = []
            for refname in sorted(repo["refs"]):
                items.append({"ref": refname, "object": {"sha": repo["refs"][refname], "type": "commit"},
                              "url": "%s/repos/%s/git/refs/%s" % (handler.base_url, repo["full_name"], refname)})
            handler.send_json(200, items)
            return True
        if len(rest) == 1 and method == "POST":
            data = body or {}
            refname = data.get("ref", "")
            sha = data.get("sha", "")
            if not refname or not object_exists(sha):
                handler.send_json(422, {"message": "Invalid ref or sha"})
                return True
            with LOCK:
                repo["refs"][refname] = sha
                log_action("create_ref", repo["full_name"], {"ref": refname})
                save_state()
            handler.send_json(201, {"ref": refname, "object": {"sha": sha, "type": "commit"}})
            return True
        if len(rest) >= 2:
            refname = "/".join(rest[1:])
            if method == "GET":
                if refname in repo["refs"]:
                    handler.send_json(200, {"ref": refname,
                                            "object": {"sha": repo["refs"][refname], "type": "commit"}})
                    return True
                handler.send_json(404, {"message": "Not Found"})
                return True
            if method == "PATCH":
                data = body or {}
                sha = data.get("sha", "")
                if refname not in repo["refs"] or not object_exists(sha):
                    handler.send_json(422, {"message": "Invalid ref or sha"})
                    return True
                with LOCK:
                    repo["refs"][refname] = sha
                    save_state()
                handler.send_json(200, {"ref": refname, "object": {"sha": sha, "type": "commit"}})
                return True
            if method == "DELETE":
                with LOCK:
                    repo["refs"].pop(refname, None)
                    save_state()
                handler.send_status(204)
                return True
        return False
    if head == "trees":
        if len(rest) == 2 and method == "GET":
            sha = rest[1]
            found = read_object(sha)
            if not found or found[0] != "tree":
                handler.send_json(404, {"message": "Not Found"})
                return True
            recursive = handler.query.get("recursive", ["0"])[0] == "1"
            entries = []
            if recursive:
                for path, (mode, oid) in sorted(tree_to_files(sha).items()):
                    entries.append({"path": path, "mode": mode, "type": "blob",
                                    "sha": oid, "size": 0, "url": ""})
            else:
                for e in parse_tree(found[1]):
                    entries.append({"path": e["name"], "mode": e["mode"],
                                    "type": "tree" if e["mode"] == "40000" else "blob",
                                    "sha": e["oid"], "url": ""})
            handler.send_json(200, {"sha": sha, "tree": entries, "truncated": False})
            return True
        if len(rest) == 1 and method == "POST":
            data = body or {}
            files = {}
            base_tree = data.get("base_tree")
            if base_tree:
                files.update(tree_to_files(base_tree))
            for item in data.get("tree", []):
                if item.get("sha"):
                    files[item["path"]] = (item.get("mode", "100644"), item["sha"])
                elif item.get("content") is not None:
                    oid = write_object("blob", item["content"].encode("utf-8"))
                    files[item["path"]] = (item.get("mode", "100644"), oid)
            oid = build_tree(files)
            handler.send_json(201, {"sha": oid, "tree": []})
            return True
        return False
    if head == "blobs":
        if len(rest) == 2 and method == "GET":
            found = read_object(rest[1])
            if not found or found[0] != "blob":
                handler.send_json(404, {"message": "Not Found"})
                return True
            handler.send_json(200, {"sha": rest[1], "size": len(found[1]),
                                    "encoding": "base64",
                                    "content": base64.b64encode(found[1]).decode("ascii")})
            return True
        if len(rest) == 1 and method == "POST":
            data = body or {}
            content = data.get("content", "")
            if data.get("encoding") == "base64":
                raw = base64.b64decode(content)
            else:
                raw = content.encode("utf-8")
            oid = write_object("blob", raw)
            handler.send_json(201, {"sha": oid, "size": len(raw)})
            return True
        return False
    if head == "commits":
        if len(rest) == 2 and method == "GET":
            found = read_object(rest[1])
            if not found or found[0] != "commit":
                handler.send_json(404, {"message": "Not Found"})
                return True
            c = parse_commit(found[1])
            handler.send_json(200, {"sha": rest[1], "tree": {"sha": c["tree"]},
                                    "parents": [{"sha": p} for p in c["parents"]],
                                    "message": c["message"],
                                    "author": {"name": "Forge", "email": "forge@localhost"},
                                    "committer": {"name": "Forge", "email": "forge@localhost"}})
            return True
        if len(rest) == 1 and method == "POST":
            data = body or {}
            message = data.get("message", "")
            tree = data.get("tree", "")
            parents = data.get("parents", [])
            if not object_exists(tree):
                handler.send_json(422, {"message": "tree not found"})
                return True
            seq = next_seq()
            oid = make_commit(tree, parents, message, seq)
            handler.send_json(201, {"sha": oid, "tree": {"sha": tree},
                                    "parents": [{"sha": p} for p in parents],
                                    "message": message})
            return True
        return False
    if head == "tags":
        if len(rest) == 1 and method == "POST":
            data = body or {}
            obj = data.get("object", "")
            if not object_exists(obj):
                handler.send_json(422, {"message": "object not found"})
                return True
            seq = next_seq()
            text = "object %s\ntype %s\ntag %s\ntagger Forge <forge@localhost> %s\n\n%s\n" % (
                obj, data.get("type", "commit"), data.get("tag", ""),
                _git_ts(seq), data.get("message", ""))
            oid = write_object("tag", text.encode("utf-8"))
            handler.send_json(201, {"sha": oid, "tag": data.get("tag", ""),
                                    "message": data.get("message", ""),
                                    "object": {"sha": obj}})
            return True
        return False
    return False


def rest_contents(handler, method, repo, path, query, body):
    ref = query.get("ref", [None])[0]
    branch = ref or repo["default_branch"]
    if method == "GET":
        start = repo["refs"].get("refs/heads/" + branch)
        if not start:
            handler.send_json(404, {"message": "Not Found"})
            return True
        coid = commit_at_ref(repo, "refs/heads/" + branch)
        if not coid:
            handler.send_json(404, {"message": "Not Found"})
            return True
        c = parse_commit(read_object(coid)[1])
        files = tree_to_files(c["tree"])
        if not path:
            items = []
            seen = set()
            for p in sorted(files):
                top = p.split("/")[0]
                if top in seen:
                    continue
                seen.add(top)
                if top in files:
                    mode, oid = files[top]
                    raw = read_object(oid)[1]
                    items.append({"type": "file", "name": top, "path": top,
                                  "sha": oid, "size": len(raw),
                                  "content": base64.b64encode(raw).decode("ascii"),
                                  "encoding": "base64"})
                else:
                    items.append({"type": "dir", "name": top, "path": top, "sha": "", "size": 0})
            handler.send_json(200, items)
            return True
        if path in files:
            mode, oid = files[path]
            raw = read_object(oid)[1]
            handler.send_json(200, {"type": "file", "name": path.split("/")[-1],
                                    "path": path, "sha": oid, "size": len(raw),
                                    "content": base64.b64encode(raw).decode("ascii"),
                                    "encoding": "base64"})
            return True
        sub = {p: v for p, v in files.items() if p.startswith(path + "/")}
        if sub:
            items = []
            seen = set()
            for p in sorted(sub):
                rest_p = p[len(path) + 1:]
                top = rest_p.split("/")[0]
                if top in seen:
                    continue
                seen.add(top)
                full = path + "/" + top
                if full in files:
                    mode, oid = files[full]
                    raw = read_object(oid)[1]
                    items.append({"type": "file", "name": top, "path": full,
                                  "sha": oid, "size": len(raw)})
                else:
                    items.append({"type": "dir", "name": top, "path": full, "sha": "", "size": 0})
            handler.send_json(200, items)
            return True
        handler.send_json(404, {"message": "Not Found"})
        return True
    if method in ("PUT", "DELETE"):
        data = body or {}
        with LOCK:
            start = repo["refs"].get("refs/heads/" + branch)
            files = {}
            parents = []
            if start:
                coid = commit_at_ref(repo, "refs/heads/" + branch)
                c = parse_commit(read_object(coid)[1])
                files = tree_to_files(c["tree"])
                parents = [coid]
            if method == "PUT":
                try:
                    raw = base64.b64decode(data.get("content", ""))
                except Exception:
                    handler.send_json(422, {"message": "content is not valid base64"})
                    return True
                oid = write_object("blob", raw)
                files[path] = ("100644", oid)
                message = data.get("message", "Update %s" % path)
            else:
                if path not in files:
                    handler.send_json(422, {"message": "file does not exist"})
                    return True
                del files[path]
                message = data.get("message", "Delete %s" % path)
            tree_oid = build_tree(files)
            seq = next_seq()
            commit_oid = make_commit(tree_oid, parents, message, seq)
            repo["refs"]["refs/heads/" + branch] = commit_oid
            log_action("contents_%s" % method.lower(), repo["full_name"], {"path": path})
            save_state()
        handler.send_json(201 if method == "PUT" else 200,
                          {"content": {"name": path.split("/")[-1], "path": path,
                                       "sha": commit_oid},
                           "commit": {"sha": commit_oid, "message": message}})
        return True
    return False


def find_issue(repo, number):
    for i in repo["issues"]:
        if i["number"] == number:
            return i
    return None


def rest_issues(handler, method, repo, rest, query, body):
    if not rest:
        if method == "GET":
            state = query.get("state", ["open"])[0]
            items = [i for i in repo["issues"] if state == "all" or i["state"] == state]
            handler.send_json(200, paginate(handler, [issue_json(handler, repo, i) for i in items]))
            return True
        if method == "POST":
            data = body or {}
            title = (data.get("title") or "").strip()
            if not title:
                handler.send_json(422, {"message": "title is required"})
                return True
            with LOCK:
                seq = next_seq()
                number = repo["next_issue"]
                repo["next_issue"] += 1
                labels = []
                for name in data.get("labels", []) or []:
                    match = [l for l in repo["labels"] if l["name"] == name]
                    labels.append(match[0] if match else {"name": name, "color": "000000"})
                issue = {"id": 1000 + number, "number": number, "title": title,
                         "body": data.get("body") or "", "state": "open",
                         "author": DEFAULT_USER, "labels": labels,
                         "assignees": data.get("assignees", []) or [],
                         "created_at": _ts(seq), "updated_at": _ts(seq),
                         "closed_at": None, "pull_request": None}
                repo["issues"].append(issue)
                log_action("create_issue", repo["full_name"], {"number": number})
                save_state()
            handler.send_json(201, issue_json(handler, repo, issue))
            return True
        return False
    try:
        number = int(rest[0])
    except ValueError:
        return False
    issue = find_issue(repo, number)
    if issue is None:
        handler.send_json(404, {"message": "Not Found"})
        return True
    if len(rest) == 1:
        if method == "GET":
            handler.send_json(200, issue_json(handler, repo, issue))
            return True
        if method == "PATCH":
            data = body or {}
            with LOCK:
                seq = next_seq()
                if "title" in data:
                    issue["title"] = data["title"]
                if "body" in data:
                    issue["body"] = data["body"]
                if "state" in data and data["state"] in ("open", "closed"):
                    issue["state"] = data["state"]
                    issue["closed_at"] = _ts(seq) if data["state"] == "closed" else None
                if "labels" in data:
                    issue["labels"] = [{"name": n, "color": "000000"}
                                       for n in (data["labels"] or [])]
                if "assignees" in data:
                    issue["assignees"] = data["assignees"] or []
                issue["updated_at"] = _ts(seq)
                log_action("update_issue", repo["full_name"], {"number": number})
                save_state()
            handler.send_json(200, issue_json(handler, repo, issue))
            return True
        return False
    if len(rest) == 2 and rest[1] == "comments":
        key = str(number)
        if method == "GET":
            items = [comment_json(handler, repo, c) for c in repo["comments"].get(key, [])]
            handler.send_json(200, paginate(handler, items))
            return True
        if method == "POST":
            data = body or {}
            if not (data.get("body") or "").strip():
                handler.send_json(422, {"message": "body is required"})
                return True
            with LOCK:
                seq = next_seq()
                cid = repo["next_comment"]
                repo["next_comment"] += 1
                comment = {"id": cid, "number": number, "body": data["body"],
                           "author": DEFAULT_USER, "created_at": _ts(seq),
                           "updated_at": _ts(seq)}
                repo["comments"].setdefault(key, []).append(comment)
                log_action("comment_issue", repo["full_name"], {"number": number})
                save_state()
            handler.send_json(201, comment_json(handler, repo, comment))
            return True
        return False
    if len(rest) == 2 and rest[1] == "labels":
        if method == "GET":
            handler.send_json(200, issue.get("labels", []))
            return True
        if method == "POST":
            data = body or {}
            names = data if isinstance(data, list) else data.get("labels", [])
            with LOCK:
                issue["labels"] = [{"name": n, "color": "000000"} for n in names]
                save_state()
            handler.send_json(200, issue.get("labels", []))
            return True
        return False
    return False


def find_pr(repo, number):
    for p in repo["pulls"]:
        if p["number"] == number:
            return p
    return None


def rest_pulls(handler, method, repo, rest, query, body):
    if not rest:
        if method == "GET":
            state = query.get("state", ["open"])[0]
            items = [p for p in repo["pulls"] if state == "all" or p["state"] == state]
            handler.send_json(200, paginate(handler, [pr_json(handler, repo, p) for p in items]))
            return True
        if method == "POST":
            data = body or {}
            title = (data.get("title") or "").strip()
            head = data.get("head", "")
            base = data.get("base", repo["default_branch"])
            if not title or not head:
                handler.send_json(422, {"message": "title and head are required"})
                return True
            if ("refs/heads/" + head) not in repo["refs"]:
                handler.send_json(422, {"message": "head ref does not exist"})
                return True
            if ("refs/heads/" + base) not in repo["refs"]:
                handler.send_json(422, {"message": "base ref does not exist"})
                return True
            with LOCK:
                seq = next_seq()
                number = repo["next_pr"]
                repo["next_pr"] += 1
                head_oid = repo["refs"]["refs/heads/" + head]
                commits = list_commits(repo, head_oid, 1000)
                pr = {"id": 2000 + number, "number": number, "title": title,
                      "body": data.get("body") or "", "state": "open",
                      "author": DEFAULT_USER, "head": head, "base": base,
                      "labels": [], "assignees": [],
                      "created_at": _ts(seq), "updated_at": _ts(seq),
                      "closed_at": None, "merged_at": None, "merged": False,
                      "merge_commit_sha": None,
                      "commits_count": len(commits)}
                repo["pulls"].append(pr)
                log_action("create_pr", repo["full_name"], {"number": number})
                save_state()
            handler.send_json(201, pr_json(handler, repo, pr))
            return True
        return False
    try:
        number = int(rest[0])
    except ValueError:
        return False
    pr = find_pr(repo, number)
    if pr is None:
        handler.send_json(404, {"message": "Not Found"})
        return True
    if len(rest) == 1:
        if method == "GET":
            handler.send_json(200, pr_json(handler, repo, pr))
            return True
        if method == "PATCH":
            data = body or {}
            with LOCK:
                seq = next_seq()
                if "title" in data:
                    pr["title"] = data["title"]
                if "body" in data:
                    pr["body"] = data["body"]
                if "state" in data and data["state"] in ("open", "closed"):
                    pr["state"] = data["state"]
                    pr["closed_at"] = _ts(seq) if data["state"] == "closed" else None
                pr["updated_at"] = _ts(seq)
                save_state()
            handler.send_json(200, pr_json(handler, repo, pr))
            return True
        return False
    if len(rest) == 2 and rest[1] == "commits" and method == "GET":
        head_oid = repo["refs"].get("refs/heads/" + pr["head"])
        items = [commit_json(handler, repo, oid, c)
                 for oid, c in list_commits(repo, head_oid, 100)] if head_oid else []
        handler.send_json(200, items)
        return True
    if len(rest) == 2 and rest[1] == "files" and method == "GET":
        handler.send_json(200, [])
        return True
    if len(rest) == 2 and rest[1] == "merge":
        if method == "GET":
            if pr.get("merged"):
                handler.send_status(204)
            else:
                handler.send_status(404)
            return True
        if method == "PUT":
            if pr["state"] != "open":
                handler.send_json(405, {"message": "Pull request is not open"})
                return True
            with LOCK:
                seq = next_seq()
                head_oid = repo["refs"].get("refs/heads/" + pr["head"])
                base_oid = repo["refs"].get("refs/heads/" + pr["base"])
                head_commit = parse_commit(read_object(head_oid)[1]) if head_oid else None
                base_commit = parse_commit(read_object(base_oid)[1]) if base_oid else None
                if head_commit is None or base_commit is None:
                    handler.send_json(422, {"message": "missing head or base commit"})
                    return True
                method = (body or {}).get("merge_method", "merge")
                if method == "rebase":
                    # Rebase replays head onto base; with linear history
                    # this is a fast-forward of base to head.
                    merge_oid = head_oid
                elif method == "squash":
                    merge_oid = make_commit(head_commit["tree"], [base_oid],
                                            "%s (#%d)" % (pr["title"], number), seq)
                else:  # "merge" (GitHub default): always a merge commit,
                    # even when a fast-forward would be possible.
                    merge_oid = make_commit(head_commit["tree"], [base_oid, head_oid],
                                            "Merge pull request #%d: %s" % (number, pr["title"]), seq)
                repo["refs"]["refs/heads/" + pr["base"]] = merge_oid
                pr["merged"] = True
                pr["state"] = "closed"
                pr["merged_at"] = _ts(seq)
                pr["closed_at"] = _ts(seq)
                pr["merge_commit_sha"] = merge_oid
                log_action("merge_pr", repo["full_name"], {"number": number})
                save_state()
            handler.send_json(200, {"sha": merge_oid, "merged": True,
                                    "message": "Pull request successfully merged"})
            return True
        return False
    if len(rest) == 2 and rest[1] == "reviews" and method == "GET":
        handler.send_json(200, [])
        return True
    if len(rest) == 2 and rest[1] == "comments":
        key = str(number)
        if method == "GET":
            items = [comment_json(handler, repo, c, "pull")
                     for c in repo["pr_comments"].get(key, [])]
            handler.send_json(200, items)
            return True
        if method == "POST":
            data = body or {}
            with LOCK:
                seq = next_seq()
                cid = repo["next_comment"]
                repo["next_comment"] += 1
                comment = {"id": cid, "number": number, "body": data.get("body", ""),
                           "author": DEFAULT_USER, "created_at": _ts(seq),
                           "updated_at": _ts(seq)}
                repo["pr_comments"].setdefault(key, []).append(comment)
                save_state()
            handler.send_json(201, comment_json(handler, repo, comment, "pull"))
            return True
        return False
    return False


# ------------------------------------------------------------- GraphQL

GQL_TOKEN_RE = re.compile(r'[\s,]*(?:(?P<str>"(?:[^"\\]|\\.)*")|(?P<name>[A-Za-z_][A-Za-z0-9_]*)|(?P<num>-?\d+(?:\.\d+)?)|(?P<punct>[!$&():=@\[\]{|}.])|(?P<ellipsis>\.\.\.))')


def gql_tokenize(text):
    toks = []
    pos = 0
    while pos < len(text):
        m = GQL_TOKEN_RE.match(text, pos)
        if not m or m.end() == pos:
            raise ValueError("GraphQL parse error at %d" % pos)
        pos = m.end()
        if m.group("str") is not None:
            toks.append(("str", json.loads(m.group("str"))))
        elif m.group("name") is not None:
            toks.append(("name", m.group("name")))
        elif m.group("num") is not None:
            toks.append(("num", m.group("num")))
        elif m.group("ellipsis") is not None:
            toks.append(("...", "..."))
        else:
            toks.append((m.group("punct"), m.group("punct")))
    toks.append(("EOF", None))
    return toks


class GqlParser:
    def __init__(self, text):
        self.toks = gql_tokenize(text)
        self.i = 0
        self.fragments = {}

    def peek(self):
        return self.toks[self.i]

    def next(self):
        tok = self.toks[self.i]
        self.i += 1
        return tok

    def expect(self, kind, value=None):
        tok = self.next()
        if tok[0] != kind or (value is not None and tok[1] != value):
            raise ValueError("expected %s %s, got %r" % (kind, value, tok))
        return tok

    def parse(self):
        ops = []
        while self.peek()[0] != "EOF":
            if self.peek() == ("name", "fragment"):
                self.next()
                name = self.next()[1]
                self.expect("name", "on")
                self.next()  # type name
                self.fragments[name] = self.parse_selection()
            else:
                op = {"kind": "query", "name": None, "selection": None}
                if self.peek() == ("name", "query") or self.peek() == ("name", "mutation"):
                    op["kind"] = self.next()[1]
                    if self.peek()[0] == "name":
                        op["name"] = self.next()[1]
                if self.peek() == ("(", "("):
                    self.parse_var_defs()
                op["selection"] = self.parse_selection()
                ops.append(op)
        return ops

    def parse_var_defs(self):
        self.expect("(", "(")
        depth = 1
        while depth:
            tok = self.next()
            if tok[0] == "(":
                depth += 1
            elif tok[0] == ")":
                depth -= 1

    def parse_selection(self):
        self.expect("{", "{")
        fields = []
        while self.peek() != ("}", "}"):
            fields.append(self.parse_field())
        self.expect("}", "}")
        return fields

    def parse_field(self):
        if self.peek()[0] == "...":
            self.next()
            if self.peek() == ("name", "on"):
                self.next()
                self.next()  # type condition
                return {"spread": "inline", "selection": self.parse_selection()}
            name = self.next()[1]
            return {"spread": name}
        name = self.next()[1]
        alias = None
        if self.peek() == (":", ":"):
            self.next()
            alias = name
            name = self.next()[1]
        args = {}
        if self.peek() == ("(", "("):
            args = self.parse_args()
        selection = None
        if self.peek() == ("{", "{"):
            selection = self.parse_selection()
        return {"name": name, "alias": alias or name, "args": args,
                "selection": selection}

    def parse_args(self):
        self.expect("(", "(")
        args = {}
        while self.peek() != (")", ")"):
            key = self.next()[1]
            self.expect(":", ":")
            args[key] = self.parse_value()
            if self.peek() == (",", ","):
                self.next()
        self.expect(")", ")")
        return args

    def parse_value(self):
        tok = self.next()
        if tok[0] == "str":
            return tok[1]
        if tok[0] == "num":
            return float(tok[1]) if "." in tok[1] else int(tok[1])
        if tok[0] == "name":
            if tok[1] == "true":
                return True
            if tok[1] == "false":
                return False
            if tok[1] == "null":
                return None
            if tok[1].startswith("$"):
                return {"$var": tok[1][1:]}
            return tok[1]
        if tok[0] == "$":
            return {"$var": self.next()[1]}
        if tok[0] == "{":
            obj = {}
            while self.peek() != ("}", "}"):
                key = self.next()[1]
                self.expect(":", ":")
                obj[key] = self.parse_value()
                if self.peek() == (",", ","):
                    self.next()
            self.next()
            return obj
        if tok[0] == "[":
            arr = []
            while self.peek() != ("]", "]"):
                arr.append(self.parse_value())
                if self.peek() == (",", ","):
                    self.next()
            self.next()
            return arr
        raise ValueError("bad GraphQL value %r" % (tok,))


def gql_subst(value, variables):
    if isinstance(value, dict):
        if "$var" in value:
            return variables.get(value["$var"])
        return {k: gql_subst(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [gql_subst(v, variables) for v in value]
    return value


def gql_resolve_fields(obj, typename, fields, handler, variables, fragments):
    out = {}
    for f in fields:
        if "spread" in f:
            if f["spread"] == "inline":
                sub = gql_resolve_fields(obj, typename, f["selection"],
                                         handler, variables, fragments)
                out.update(sub)
            elif f["spread"] in fragments:
                sub = gql_resolve_fields(obj, typename, fragments[f["spread"]],
                                         handler, variables, fragments)
                out.update(sub)
            continue
        args = {k: gql_subst(v, variables) for k, v in f.get("args", {}).items()}
        out[f["alias"]] = gql_resolve_field(obj, typename, f["name"], args,
                                            f.get("selection"), handler,
                                            variables, fragments)
    return out


def _gql_conn(items, fields, handler, variables, fragments, total=None):
    total = len(items) if total is None else total
    first = None
    if fields:
        for f in fields:
            if f.get("name") == "totalCount":
                pass
    result = {"totalCount": total}
    if fields:
        for f in fields:
            if f.get("name") == "nodes":
                result["nodes"] = items
                # nodes are resolved by the caller via sub-selection
                result["_nodes_sel"] = f.get("selection")
                result["_nodes_items"] = items
            elif f.get("name") == "edges":
                result["edges"] = [{"node": it, "cursor": str(i)}
                                   for i, it in enumerate(items)]
                result["_edges_sel"] = f.get("selection")
            elif f.get("name") == "pageInfo":
                result["pageInfo"] = {"hasNextPage": False, "hasPreviousPage": False,
                                      "startCursor": None, "endCursor": None}
    return result


def gql_issue_fields(issue, repo, handler):
    return {
        "id": "I_%d" % issue["id"], "number": issue["number"],
        "title": issue["title"], "body": issue.get("body") or "",
        "state": issue["state"].upper(),
        "createdAt": issue["created_at"], "updatedAt": issue["updated_at"],
        "closedAt": issue.get("closed_at"),
        "url": "%s/%s/issues/%d" % (handler.base_url, repo["full_name"], issue["number"]),
        "author": {"login": issue.get("author", DEFAULT_USER)},
        "labels": _gql_conn([{"name": l["name"], "color": l.get("color", "")}
                             for l in issue.get("labels", [])], None, handler, {}, {}),
        "comments": _gql_conn(repo["comments"].get(str(issue["number"]), []),
                              None, handler, {}, {}),
        "repository": None,  # filled by caller when needed
    }


def gql_pr_fields(pr, repo, handler):
    return {
        "id": "PR_%d" % pr["id"], "number": pr["number"],
        "title": pr["title"], "body": pr.get("body") or "",
        "state": pr["state"].upper(),
        "createdAt": pr["created_at"], "updatedAt": pr["updated_at"],
        "closedAt": pr.get("closed_at"), "mergedAt": pr.get("merged_at"),
        "merged": bool(pr.get("merged")), "mergeable": True,
        "headRefName": pr["head"], "baseRefName": pr["base"],
        "url": "%s/%s/pull/%d" % (handler.base_url, repo["full_name"], pr["number"]),
        "author": {"login": pr.get("author", DEFAULT_USER)},
        "labels": _gql_conn([], None, handler, {}, {}),
        "comments": _gql_conn(repo["pr_comments"].get(str(pr["number"]), []),
                              None, handler, {}, {}),
    }


def gql_repo_fields(repo, handler):
    return {
        "id": "R_%d" % repo["id"], "name": repo["name"],
        "nameWithOwner": repo["full_name"], "description": repo["description"],
        "url": "%s/%s" % (handler.base_url, repo["full_name"]),
        "createdAt": repo["created_at"], "updatedAt": repo["updated_at"],
        "isPrivate": repo["private"], "isFork": False, "forkCount": 0,
        "stargazerCount": 0, "watcherCount": 0,
        "defaultBranchRef": {"name": repo["default_branch"]},
        "owner": {"login": repo["owner"], "id": "U_%d" % repo["id"]},
        "licenseInfo": None, "primaryLanguage": None,
    }


def gql_resolve_field(obj, typename, name, args, selection, handler, variables, fragments):
    # obj is a plain dict of precomputed field values, or a marker dict
    # with "_kind" for lazy connections.
    if isinstance(obj, dict) and obj.get("_kind") == "repo":
        repo = obj["repo"]
        base = gql_repo_fields(repo, handler)
        if name in base:
            val = base[name]
            if selection and isinstance(val, dict):
                return gql_resolve_fields(val, name, selection, handler, variables, fragments)
            return val
        if name == "issues":
            items = [i for i in repo["issues"]
                     if not args.get("states") or i["state"].upper() in [str(s).upper() for s in args["states"]]]
            first = args.get("first")
            if first is not None:
                items = items[:first]
            return gql_connection([gql_issue_fields(i, repo, handler) for i in items],
                                  selection, handler, variables, fragments, "Issue")
        if name == "pullRequests":
            items = [p for p in repo["pulls"]
                     if not args.get("states") or p["state"].upper() in [str(s).upper() for s in args["states"]]]
            first = args.get("first")
            if first is not None:
                items = items[:first]
            return gql_connection([gql_pr_fields(p, repo, handler) for p in items],
                                  selection, handler, variables, fragments, "PullRequest")
        if name == "labels":
            items = [{"name": l["name"], "color": l.get("color", "")} for l in repo["labels"]]
            return gql_connection(items, selection, handler, variables, fragments, "Label")
        if name == "issues_connection":
            return None
        if selection:
            return {}
        return None
    if isinstance(obj, dict) and obj.get("_kind") == "issue":
        issue, repo = obj["issue"], obj["repo"]
        base = gql_issue_fields(issue, repo, handler)
        if name == "repository":
            if selection:
                return gql_resolve_fields({"_kind": "repo", "repo": repo}, "Repository",
                                          selection, handler, variables, fragments)
            return repo["full_name"]
        if name in base:
            val = base[name]
            if name == "labels" and selection:
                return gql_connection([{"name": l["name"], "color": l.get("color", "")}
                                       for l in issue.get("labels", [])],
                                      selection, handler, variables, fragments, "Label")
            if name == "comments" and selection:
                return gql_connection([{"body": c["body"],
                                        "author": {"login": c.get("author", DEFAULT_USER)},
                                        "createdAt": c["created_at"]}
                                       for c in repo["comments"].get(str(issue["number"]), [])],
                                      selection, handler, variables, fragments, "IssueComment")
            if selection and isinstance(val, dict):
                return gql_resolve_fields(val, name, selection, handler, variables, fragments)
            return val
        if selection:
            return {}
        return None
    if isinstance(obj, dict) and obj.get("_kind") == "pr":
        pr, repo = obj["pr"], obj["repo"]
        base = gql_pr_fields(pr, repo, handler)
        if name in base:
            val = base[name]
            if selection and isinstance(val, dict):
                return gql_resolve_fields(val, name, selection, handler, variables, fragments)
            return val
        if selection:
            return {}
        return None
    if isinstance(obj, dict):
        # generic nested object (author, owner, pageInfo, ...)
        if name in obj:
            val = obj[name]
            if selection and isinstance(val, dict):
                return gql_resolve_fields(val, name, selection, handler, variables, fragments)
            if selection and isinstance(val, list):
                return val
            return val
        if selection:
            return {}
        return None
    return None


def gql_connection(items, selection, handler, variables, fragments, item_kind):
    """Resolve a GraphQL connection (totalCount/nodes/edges/pageInfo)."""
    out = {"totalCount": len(items)}
    if not selection:
        return out
    for f in selection:
        if "spread" in f:
            continue
        fname = f["name"]
        if fname == "totalCount":
            out["totalCount"] = len(items)
        elif fname == "nodes":
            nodes = []
            for it in items:
                if isinstance(it, dict) and "_kind" in it:
                    nodes.append(gql_resolve_fields(it, item_kind, f.get("selection") or [],
                                                    handler, variables, fragments))
                elif isinstance(it, dict):
                    nodes.append(gql_resolve_fields(it, item_kind, f.get("selection") or [],
                                                    handler, variables, fragments))
                else:
                    nodes.append(it)
            out["nodes"] = nodes
        elif fname == "edges":
            edges = []
            for i, it in enumerate(items):
                edges.append({"node": it, "cursor": str(i)})
            # Resolve node sub-selections properly.
            if f.get("selection"):
                for edge, it in zip(edges, items):
                    for sf in f["selection"]:
                        if sf.get("name") == "node" and sf.get("selection"):
                            edge["node"] = gql_resolve_fields(
                                it, item_kind, sf["selection"], handler, variables, fragments)
            out["edges"] = edges
        elif fname == "pageInfo":
            out["pageInfo"] = {"hasNextPage": False, "hasPreviousPage": False,
                               "startCursor": None, "endCursor": None}
    return out


def gql_root(handler, op, variables, fragments):
    data = {}
    errors = []
    for f in op["selection"]:
        if "spread" in f:
            continue
        name = f["name"]
        args = {k: gql_subst(v, variables) for k, v in f.get("args", {}).items()}
        sel = f.get("selection")
        try:
            if op["kind"] == "mutation":
                data[f["alias"]] = gql_mutation(handler, name, args, sel, variables, fragments)
            elif name == "repository":
                repo = get_repo(args.get("owner", ""), args.get("name", ""))
                if repo is None:
                    data[f["alias"]] = None
                else:
                    data[f["alias"]] = gql_resolve_fields(
                        {"_kind": "repo", "repo": repo}, "Repository", sel or [],
                        handler, variables, fragments)
            elif name == "viewer":
                val = {"login": DEFAULT_USER, "id": "U_1"}
                data[f["alias"]] = gql_resolve_fields(val, "User", sel or [],
                                                      handler, variables, fragments)
            elif name == "user":
                val = {"login": args.get("login", DEFAULT_USER), "id": "U_2"}
                if sel:
                    extra = {}
                    if any(s.get("name") == "repositories" for s in sel):
                        items = [r for r in STATE["repos"].values()
                                 if r["owner"] == args.get("login")]
                        for s in sel:
                            if s.get("name") == "repositories":
                                first = (s.get("args") or {}).get("first")
                                conn_items = [{"_kind": "repo", "repo": r} for r in items]
                                if first is not None:
                                    conn_items = conn_items[:first]
                                extra["repositories"] = gql_connection(
                                    conn_items, s.get("selection"), handler,
                                    variables, fragments, "Repository")
                    val.update(extra)
                data[f["alias"]] = gql_resolve_fields(val, "User", sel or [],
                                                      handler, variables, fragments)
            elif name == "node":
                data[f["alias"]] = None
            elif name == "search":
                data[f["alias"]] = {"issueCount": 0, "repositoryCount": 0,
                                    "edges": [], "nodes": []}
            else:
                data[f["alias"]] = None
        except Exception as exc:  # never fail the whole document
            errors.append({"message": str(exc), "path": [name]})
            data[f["alias"]] = None
    return data, errors


def _repo_from_id(repo_id):
    if not isinstance(repo_id, str):
        return None
    if repo_id.startswith("R_"):
        try:
            rid = int(repo_id[2:])
        except ValueError:
            return None
        for r in STATE["repos"].values():
            if r["id"] == rid:
                return r
        return None
    if "/" in repo_id:
        owner, _, name = repo_id.partition("/")
        return get_repo(owner, name)
    return None


def gql_mutation(handler, name, args, selection, variables, fragments):
    inp = args.get("input") or {}
    if isinstance(inp, dict):
        inp = {k: gql_subst(v, variables) for k, v in inp.items()}
    if name == "createIssue":
        repo = _repo_from_id(inp.get("repositoryId", "")) or get_repo(
            inp.get("owner", ""), inp.get("name", ""))
        if repo is None:
            raise ValueError("repository not found")
        title = (inp.get("title") or "").strip()
        if not title:
            raise ValueError("title is required")
        with LOCK:
            seq = next_seq()
            number = repo["next_issue"]
            repo["next_issue"] += 1
            issue = {"id": 1000 + number, "number": number, "title": title,
                     "body": inp.get("body") or "", "state": "open",
                     "author": DEFAULT_USER, "labels": [],
                     "assignees": inp.get("assignees") or [],
                     "created_at": _ts(seq), "updated_at": _ts(seq),
                     "closed_at": None, "pull_request": None}
            repo["issues"].append(issue)
            log_action("create_issue", repo["full_name"], {"number": number})
            save_state()
        result = {"clientMutationId": inp.get("clientMutationId")}
        if selection:
            for f in selection:
                if f.get("name") == "issue" and f.get("selection"):
                    result["issue"] = gql_resolve_fields(
                        {"_kind": "issue", "issue": issue, "repo": repo},
                        "Issue", f["selection"], handler, variables, fragments)
        return result
    if name == "createPullRequest":
        repo = _repo_from_id(inp.get("repositoryId", "")) or get_repo(
            inp.get("owner", ""), inp.get("name", ""))
        if repo is None:
            raise ValueError("repository not found")
        head = inp.get("headRefName", "")
        base = inp.get("baseRefName", repo["default_branch"])
        if ("refs/heads/" + head) not in repo["refs"]:
            raise ValueError("head ref does not exist")
        with LOCK:
            seq = next_seq()
            number = repo["next_pr"]
            repo["next_pr"] += 1
            pr = {"id": 2000 + number, "number": number,
                  "title": inp.get("title", ""), "body": inp.get("body") or "",
                  "state": "open", "author": DEFAULT_USER, "head": head,
                  "base": base, "labels": [], "assignees": [],
                  "created_at": _ts(seq), "updated_at": _ts(seq),
                  "closed_at": None, "merged_at": None, "merged": False,
                  "merge_commit_sha": None, "commits_count": 0}
            repo["pulls"].append(pr)
            log_action("create_pr", repo["full_name"], {"number": number})
            save_state()
        result = {"clientMutationId": inp.get("clientMutationId")}
        if selection:
            for f in selection:
                if f.get("name") == "pullRequest" and f.get("selection"):
                    result["pullRequest"] = gql_resolve_fields(
                        {"_kind": "pr", "pr": pr, "repo": repo},
                        "PullRequest", f["selection"], handler, variables, fragments)
        return result
    if name in ("updateIssue", "closeIssue"):
        issue_id = inp.get("id") or inp.get("issueId") or ""
        target = None
        for r in STATE["repos"].values():
            for i in r["issues"]:
                if "I_%d" % i["id"] == issue_id or str(i["number"]) == str(issue_id):
                    target = (r, i)
                    break
        if target is None:
            raise ValueError("issue not found")
        repo, issue = target
        with LOCK:
            seq = next_seq()
            state = (inp.get("state") or "").upper()
            if name == "closeIssue" or state == "CLOSED":
                issue["state"] = "closed"
                issue["closed_at"] = _ts(seq)
            elif state == "OPEN":
                issue["state"] = "open"
                issue["closed_at"] = None
            if "title" in inp:
                issue["title"] = inp["title"]
            if "body" in inp:
                issue["body"] = inp["body"]
            issue["updated_at"] = _ts(seq)
            save_state()
        result = {"clientMutationId": inp.get("clientMutationId")}
        if selection:
            for f in selection:
                if f.get("name") == "issue" and f.get("selection"):
                    result["issue"] = gql_resolve_fields(
                        {"_kind": "issue", "issue": issue, "repo": repo},
                        "Issue", f["selection"], handler, variables, fragments)
        return result
    if name == "addComment":
        subject = inp.get("subjectId", "")
        body = inp.get("body", "")
        target = None
        for r in STATE["repos"].values():
            for i in r["issues"]:
                if "I_%d" % i["id"] == subject:
                    target = (r, i)
                    break
        if target is None:
            raise ValueError("subject not found")
        repo, issue = target
        with LOCK:
            seq = next_seq()
            cid = repo["next_comment"]
            repo["next_comment"] += 1
            comment = {"id": cid, "number": issue["number"], "body": body,
                       "author": DEFAULT_USER, "created_at": _ts(seq),
                       "updated_at": _ts(seq)}
            repo["comments"].setdefault(str(issue["number"]), []).append(comment)
            save_state()
        result = {"clientMutationId": inp.get("clientMutationId")}
        if selection:
            for f in selection:
                if f.get("name") == "commentEdge" and f.get("selection"):
                    node_sel = None
                    for sf in f["selection"]:
                        if sf.get("name") == "node":
                            node_sel = sf.get("selection")
                    node = {"body": body, "author": {"login": DEFAULT_USER},
                            "createdAt": _ts(seq)}
                    result["commentEdge"] = {
                        "node": gql_resolve_fields(node, "IssueComment",
                                                   node_sel or [], handler,
                                                   variables, fragments)}
        return result
    raise ValueError("unknown mutation %s" % name)


# ------------------------------------------------------------------ MCP

MCP_TOOLS = [
    {"name": "list_repositories", "description": "List repositories owned by a user",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}}, "required": []}},
    {"name": "get_repository", "description": "Get a repository by owner and name",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}}, "required": ["owner", "name"]}},
    {"name": "create_repository", "description": "Create a repository",
     "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}, "description": {"type": "string"}, "owner": {"type": "string"}}, "required": ["name"]}},
    {"name": "list_issues", "description": "List issues of a repository",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "state": {"type": "string"}}, "required": ["owner", "name"]}},
    {"name": "get_issue", "description": "Get one issue by number",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "number": {"type": "integer"}}, "required": ["owner", "name", "number"]}},
    {"name": "create_issue", "description": "Create an issue",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "title": {"type": "string"}, "body": {"type": "string"}}, "required": ["owner", "name", "title"]}},
    {"name": "update_issue", "description": "Update an issue (title, body, state)",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "number": {"type": "integer"}, "title": {"type": "string"}, "body": {"type": "string"}, "state": {"type": "string"}}, "required": ["owner", "name", "number"]}},
    {"name": "add_issue_comment", "description": "Add a comment to an issue",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "number": {"type": "integer"}, "body": {"type": "string"}}, "required": ["owner", "name", "number", "body"]}},
    {"name": "list_pull_requests", "description": "List pull requests of a repository",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "state": {"type": "string"}}, "required": ["owner", "name"]}},
    {"name": "create_pull_request", "description": "Create a pull request",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "title": {"type": "string"}, "head": {"type": "string"}, "base": {"type": "string"}, "body": {"type": "string"}}, "required": ["owner", "name", "title", "head"]}},
    {"name": "merge_pull_request", "description": "Merge a pull request",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "number": {"type": "integer"}}, "required": ["owner", "name", "number"]}},
    {"name": "get_file_contents", "description": "Get file contents (base64) from a branch",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "path": {"type": "string"}, "ref": {"type": "string"}}, "required": ["owner", "name", "path"]}},
    {"name": "create_or_update_file", "description": "Create or update a file on a branch",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "path": {"type": "string"}, "content": {"type": "string"}, "message": {"type": "string"}, "branch": {"type": "string"}}, "required": ["owner", "name", "path", "content"]}},
    {"name": "list_branches", "description": "List branches of a repository",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}}, "required": ["owner", "name"]}},
    {"name": "list_commits", "description": "List commits on a branch",
     "inputSchema": {"type": "object", "properties": {"owner": {"type": "string"}, "name": {"type": "string"}, "branch": {"type": "string"}}, "required": ["owner", "name"]}},
    {"name": "search_issues", "description": "Search issues by text",
     "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}},
    {"name": "search_repositories", "description": "Search repositories by text",
     "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}},
]


def mcp_call(handler, tool, args):
    owner = args.get("owner", DEFAULT_USER)
    name = args.get("name", "")

    def need_repo():
        repo = get_repo(owner, name)
        if repo is None:
            raise ValueError("repository not found: %s/%s" % (owner, name))
        return repo

    if tool == "list_repositories":
        items = [repo_json(handler, r) for r in STATE["repos"].values()
                 if not args.get("owner") or r["owner"] == args["owner"]]
        return items
    if tool == "get_repository":
        return repo_json(handler, need_repo())
    if tool == "create_repository":
        if get_repo(owner, name):
            raise ValueError("repository already exists")
        with LOCK:
            repo = create_repo_record(owner, name, args.get("description", ""))
        return repo_json(handler, repo)
    if tool == "list_issues":
        repo = need_repo()
        state = args.get("state", "open")
        return [issue_json(handler, repo, i) for i in repo["issues"]
                if state == "all" or i["state"] == state]
    if tool == "get_issue":
        repo = need_repo()
        issue = find_issue(repo, int(args["number"]))
        if issue is None:
            raise ValueError("issue not found")
        return issue_json(handler, repo, issue)
    if tool == "create_issue":
        repo = need_repo()
        with LOCK:
            seq = next_seq()
            number = repo["next_issue"]
            repo["next_issue"] += 1
            issue = {"id": 1000 + number, "number": number,
                     "title": args["title"], "body": args.get("body") or "",
                     "state": "open", "author": DEFAULT_USER, "labels": [],
                     "assignees": [], "created_at": _ts(seq),
                     "updated_at": _ts(seq), "closed_at": None,
                     "pull_request": None}
            repo["issues"].append(issue)
            log_action("create_issue", repo["full_name"], {"number": number})
            save_state()
        return issue_json(handler, repo, issue)
    if tool == "update_issue":
        repo = need_repo()
        issue = find_issue(repo, int(args["number"]))
        if issue is None:
            raise ValueError("issue not found")
        with LOCK:
            seq = next_seq()
            if "title" in args:
                issue["title"] = args["title"]
            if "body" in args:
                issue["body"] = args["body"]
            if "state" in args and args["state"] in ("open", "closed"):
                issue["state"] = args["state"]
                issue["closed_at"] = _ts(seq) if args["state"] == "closed" else None
            issue["updated_at"] = _ts(seq)
            save_state()
        return issue_json(handler, repo, issue)
    if tool == "add_issue_comment":
        repo = need_repo()
        issue = find_issue(repo, int(args["number"]))
        if issue is None:
            raise ValueError("issue not found")
        with LOCK:
            seq = next_seq()
            cid = repo["next_comment"]
            repo["next_comment"] += 1
            comment = {"id": cid, "number": issue["number"], "body": args["body"],
                       "author": DEFAULT_USER, "created_at": _ts(seq),
                       "updated_at": _ts(seq)}
            repo["comments"].setdefault(str(issue["number"]), []).append(comment)
            save_state()
        return comment_json(handler, repo, comment)
    if tool == "list_pull_requests":
        repo = need_repo()
        state = args.get("state", "open")
        return [pr_json(handler, repo, p) for p in repo["pulls"]
                if state == "all" or p["state"] == state]
    if tool == "create_pull_request":
        repo = need_repo()
        head = args["head"]
        base = args.get("base", repo["default_branch"])
        if ("refs/heads/" + head) not in repo["refs"]:
            raise ValueError("head ref does not exist")
        with LOCK:
            seq = next_seq()
            number = repo["next_pr"]
            repo["next_pr"] += 1
            pr = {"id": 2000 + number, "number": number, "title": args["title"],
                  "body": args.get("body") or "", "state": "open",
                  "author": DEFAULT_USER, "head": head, "base": base,
                  "labels": [], "assignees": [], "created_at": _ts(seq),
                  "updated_at": _ts(seq), "closed_at": None, "merged_at": None,
                  "merged": False, "merge_commit_sha": None, "commits_count": 0}
            repo["pulls"].append(pr)
            log_action("create_pr", repo["full_name"], {"number": number})
            save_state()
        return pr_json(handler, repo, pr)
    if tool == "merge_pull_request":
        repo = need_repo()
        pr = find_pr(repo, int(args["number"]))
        if pr is None or pr["state"] != "open":
            raise ValueError("pull request not open")
        with LOCK:
            seq = next_seq()
            head_oid = repo["refs"].get("refs/heads/" + pr["head"])
            base_oid = repo["refs"].get("refs/heads/" + pr["base"])
            head_commit = parse_commit(read_object(head_oid)[1])
            merge_oid = make_commit(head_commit["tree"], [base_oid, head_oid],
                                    "Merge pull request #%d" % pr["number"], seq)
            repo["refs"]["refs/heads/" + pr["base"]] = merge_oid
            pr["merged"] = True
            pr["state"] = "closed"
            pr["merged_at"] = _ts(seq)
            pr["closed_at"] = _ts(seq)
            pr["merge_commit_sha"] = merge_oid
            save_state()
        return {"merged": True, "sha": merge_oid}
    if tool == "get_file_contents":
        repo = need_repo()
        branch = args.get("ref") or repo["default_branch"]
        coid = commit_at_ref(repo, "refs/heads/" + branch)
        if not coid:
            raise ValueError("branch not found")
        files = tree_to_files(parse_commit(read_object(coid)[1])["tree"])
        path = args["path"]
        if path not in files:
            raise ValueError("file not found")
        mode, oid = files[path]
        raw = read_object(oid)[1]
        return {"path": path, "sha": oid, "size": len(raw),
                "content": base64.b64encode(raw).decode("ascii"),
                "encoding": "base64"}
    if tool == "create_or_update_file":
        repo = need_repo()
        branch = args.get("branch") or repo["default_branch"]
        raw = base64.b64decode(args["content"]) if args.get("encoding") == "base64" \
            else args["content"].encode("utf-8")
        with LOCK:
            files = {}
            parents = []
            start = repo["refs"].get("refs/heads/" + branch)
            if start:
                coid = commit_at_ref(repo, "refs/heads/" + branch)
                files = tree_to_files(parse_commit(read_object(coid)[1])["tree"])
                parents = [coid]
            oid = write_object("blob", raw)
            files[args["path"]] = ("100644", oid)
            tree_oid = build_tree(files)
            seq = next_seq()
            commit_oid = make_commit(tree_oid, parents,
                                     args.get("message", "Update %s" % args["path"]), seq)
            repo["refs"]["refs/heads/" + branch] = commit_oid
            save_state()
        return {"path": args["path"], "sha": commit_oid}
    if tool == "list_branches":
        repo = need_repo()
        return [{"name": r[len("refs/heads/"):], "sha": repo["refs"][r]}
                for r in sorted(repo["refs"]) if r.startswith("refs/heads/")]
    if tool == "list_commits":
        repo = need_repo()
        branch = args.get("branch") or repo["default_branch"]
        start = repo["refs"].get("refs/heads/" + branch)
        return [commit_json(handler, repo, oid, c)
                for oid, c in list_commits(repo, start, 30)] if start else []
    if tool == "search_issues":
        needle = args["query"].lower()
        out = []
        for r in STATE["repos"].values():
            for i in r["issues"]:
                if needle in i["title"].lower() or needle in (i.get("body") or "").lower():
                    out.append(issue_json(handler, r, i))
        return out
    if tool == "search_repositories":
        needle = args["query"].lower()
        return [repo_json(handler, r) for r in STATE["repos"].values()
                if needle in r["full_name"].lower() or needle in r["description"].lower()]
    raise ValueError("unknown tool: %s" % tool)


def mcp_dispatch(handler, body):
    req = body or {}
    if not isinstance(req, dict):
        return {"jsonrpc": "2.0", "id": None,
                "error": {"code": -32600, "message": "Invalid Request"}}
    method = req.get("method", "")
    rid = req.get("id")
    params = req.get("params") or {}
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid,
                "result": {"protocolVersion": req.get("params", {}).get("protocolVersion", "2024-11-05"),
                           "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                           "serverInfo": {"name": "mini-forge", "version": VERSION}}}
    if method in ("notifications/initialized", "notifications/cancelled"):
        return None  # notification: no response
    if method == "ping":
        return {"jsonrpc": "2.0", "id": rid, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid,
                "result": {"tools": MCP_TOOLS}}
    if method == "tools/call":
        name = params.get("name", "")
        args = params.get("arguments") or {}
        try:
            result = mcp_call(handler, name, args)
        except Exception as exc:
            return {"jsonrpc": "2.0", "id": rid,
                    "result": {"content": [{"type": "text", "text": "Error: %s" % exc}],
                               "isError": True}}
        return {"jsonrpc": "2.0", "id": rid,
                "result": {"content": [{"type": "text",
                                        "text": json.dumps(result, sort_keys=True)}]}}
    if method == "resources/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"resources": []}}
    if method == "prompts/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"prompts": []}}
    return {"jsonrpc": "2.0", "id": rid,
            "error": {"code": -32601, "message": "Method not found: %s" % method}}


# ------------------------------------------------------- state export

def state_export():
    repos = {}
    for full in sorted(STATE["repos"]):
        repo = STATE["repos"][full]
        reachable = set()
        for refname, oid in repo["refs"].items():
            reachable |= reachable_oids([oid])
        issues = []
        for i in sorted(repo["issues"], key=lambda x: x["number"]):
            item = {"number": i["number"], "title": i["title"],
                    "body": i.get("body") or "", "state": i["state"],
                    "author": i.get("author", DEFAULT_USER),
                    "labels": sorted(l["name"] for l in i.get("labels", [])),
                    "assignees": sorted(i.get("assignees", [])),
                    "created_at": i["created_at"], "updated_at": i["updated_at"],
                    "closed_at": i.get("closed_at"),
                    "comments": [{"body": c["body"], "author": c.get("author", DEFAULT_USER),
                                  "created_at": c["created_at"]}
                                 for c in repo["comments"].get(str(i["number"]), [])]}
            issues.append(item)
        pulls = []
        for p in sorted(repo["pulls"], key=lambda x: x["number"]):
            pulls.append({"number": p["number"], "title": p["title"],
                          "body": p.get("body") or "", "state": p["state"],
                          "head": p["head"], "base": p["base"],
                          "merged": bool(p.get("merged")),
                          "merge_commit_sha": p.get("merge_commit_sha"),
                          "created_at": p["created_at"],
                          "updated_at": p["updated_at"]})
        repos[full] = {"name": repo["name"], "owner": repo["owner"],
                       "description": repo["description"],
                       "default_branch": repo["default_branch"],
                       "refs": {k: repo["refs"][k] for k in sorted(repo["refs"])},
                       "objects": sorted(reachable),
                       "issues": issues, "pulls": pulls}
    return {"version": 1, "forge": "mini-forge/%s" % VERSION,
            "repos": repos, "actions": STATE["actions"]}


# ------------------------------------------------------------ HTTP server

class ForgeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MiniForge/" + VERSION

    @property
    def base_url(self):
        return "http://127.0.0.1:%d" % self.server.server_port

    def log_message(self, fmt, *args):
        pass  # keep stdout for FORGE_PORT only

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _parse_json_body(self, raw):
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except ValueError:
            return {}

    def send_bytes(self, status, data, ctype="application/octet-stream"):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def send_json(self, status, obj):
        data = json.dumps(obj, sort_keys=True).encode("utf-8")
        self.send_bytes(status, data, "application/json")

    def send_status(self, status):
        self.send_bytes(status, b"", "application/json")

    def _route(self, method):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        raw = self._read_body() if method in ("POST", "PUT", "PATCH", "DELETE") else b""
        body = self._parse_json_body(raw) if raw else {}
        # --- git smart HTTP (with or without .git suffix) ---
        m = re.match(r"^/([^/]+)/([^/]+?)(?:\.git)?/(info/refs|git-upload-pack|git-receive-pack)$", path)
        if m:
            owner, name, action = m.group(1), m.group(2), m.group(3)
            if action == "info/refs":
                service = query.get("service", [None])[0]
                handle_info_refs(self, owner, name, service)
            elif action == "git-upload-pack" and method == "POST":
                handle_upload_pack(self, owner, name, raw)
            elif action == "git-receive-pack" and method == "POST":
                handle_receive_pack(self, owner, name, raw)
            else:
                self.send_json(405, {"message": "Method Not Allowed"})
            return
        m = re.match(r"^/([^/]+)/([^/]+?)(?:\.git)?/HEAD$", path)
        if m and method == "GET":
            repo = get_repo(m.group(1), m.group(2))
            if repo is None:
                self.send_json(404, {"message": "Not Found"})
            else:
                self.send_bytes(200, ("ref: refs/heads/%s\n" % repo["default_branch"]).encode(),
                                "text/plain")
            return
        # --- state export / health ---
        if path in ("/__export", "/__state", "/state", "/export") and method == "GET":
            self.send_json(200, state_export())
            return
        if path == "/__reset" and method == "POST":
            with LOCK:
                fresh_start()
            self.send_json(200, {"reset": True})
            return
        if path == "/health" and method == "GET":
            self.send_json(200, {"status": "ok", "forge": "mini-forge/%s" % VERSION})
            return
        # --- GraphQL ---
        if path in ("/graphql", "/api/graphql") and method == "POST":
            query_text = body.get("query", "")
            variables = body.get("variables") or {}
            try:
                parser = GqlParser(query_text)
                ops = parser.parse()
            except ValueError as exc:
                self.send_json(400, {"errors": [{"message": str(exc)}]})
                return
            data = {}
            errors = []
            for op in ops:
                d, e = gql_root(self, op, variables, parser.fragments)
                data.update(d)
                errors.extend(e)
            out = {"data": data}
            if errors:
                out["errors"] = errors
            self.send_json(200, out)
            return
        # --- MCP ---
        if path in ("/mcp", "/mcp/") and method == "POST":
            resp = mcp_dispatch(self, body)
            if resp is None:
                self.send_bytes(202, b"", "application/json")
            else:
                self.send_json(200, resp)
            return
        if path in ("/mcp", "/mcp/") and method == "GET":
            self.send_json(405, {"message": "MCP requires POST with JSON-RPC"})
            return
        # --- REST v3 ---
        if rest_dispatch(self, method, path, query, body):
            return
        self.send_json(404, {"message": "Not Found", "path": path})

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def do_PUT(self):
        self._route("PUT")

    def do_PATCH(self):
        self._route("PATCH")

    def do_DELETE(self):
        self._route("DELETE")


def register_port(port):
    try:
        with open("forge.port", "w", encoding="utf-8") as fh:
            fh.write(str(port))
    except OSError:
        pass
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8080/forge",
            data=json.dumps({"port": port}).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=2) as resp:
            resp.read()
    except Exception:
        pass  # suite may not be listening yet; stdout + file suffice


def main():
    fresh_start()
    os.makedirs(os.path.join(DATA_DIR, "objects"), exist_ok=True)
    port = 0
    env_port = os.environ.get("FORGE_PORT", "")
    if env_port.isdigit() and int(env_port) != 8080 and int(env_port) > 0:
        port = int(env_port)
    server = ThreadingHTTPServer(("127.0.0.1", port), ForgeHandler)
    server.daemon_threads = True
    actual = server.server_address[1]
    print("FORGE_PORT=%d" % actual, flush=True)
    print("MiniForge %s listening on http://127.0.0.1:%d" % (VERSION, actual), flush=True)
    register_port(actual)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        save_state()


if __name__ == "__main__":
    main()
