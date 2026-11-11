#!/usr/bin/env python3
"""Listen to Wikimedia EventStreams for a few seconds, print events and save them to a text file.

Docs: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

Examples:
    python3 wiki_stream.py                      # 3 seconds of all recent changes -> wiki_stream_output.txt
    python3 wiki_stream.py --duration 10 --output my_events.txt
    python3 wiki_stream.py --wiki enwiki        # only English Wikipedia
    python3 wiki_stream.py --no-bots --limit 20
    python3 wiki_stream.py --stream page-create --raw
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE_URL = "https://stream.wikimedia.org/v2/stream/"
# Wikimedia asks clients to send a descriptive User-Agent; requests without one may be blocked.
# Replace the contact part with your own email or project URL.
USER_AGENT = "wiki-stream-demo/0.1 (contact: you@example.com) python-urllib"
# Project root (one level above test_projects/).
DEFAULT_OUTPUT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "wiki_stream_output.txt")


def iter_sse_events(url, last_event_id=None):
    """Yield (event_id, data) tuples from a Server-Sent Events endpoint."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as resp:
        event_id, data_lines = None, []
        for raw in resp:
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line:
                # Blank line terminates an event.
                if data_lines:
                    yield event_id, "\n".join(data_lines)
                event_id, data_lines = None, []
            elif line.startswith(":"):
                continue  # comment / keep-alive
            else:
                field, _, value = line.partition(":")
                value = value[1:] if value.startswith(" ") else value
                if field == "data":
                    data_lines.append(value)
                elif field == "id":
                    event_id = value


def format_recentchange(ev):
    ts = time.strftime("%H:%M:%S", time.localtime(ev.get("timestamp", time.time())))
    kind = ev.get("type", "?")
    bot = " [bot]" if ev.get("bot") else ""
    line = f"{ts} {ev.get('wiki', '?'):<14} {kind:<10} {ev.get('title', '')!r} by {ev.get('user', '?')}{bot}"

    length = ev.get("length") or {}
    if "old" in length or "new" in length:
        delta = (length.get("new") or 0) - (length.get("old") or 0)
        line += f" ({delta:+d} bytes)"
    if ev.get("comment"):
        line += f" — {ev['comment'][:80]}"
    return line


def matches(ev, args):
    if args.wiki and ev.get("wiki", ev.get("database")) != args.wiki:
        return False
    if args.no_bots and ev.get("bot", ev.get("performer", {}).get("user_is_bot")):
        return False
    if args.type and ev.get("type") != args.type:
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", default="recentchange",
                        help="stream name, e.g. recentchange, page-create, revision-create (default: recentchange)")
    parser.add_argument("--wiki", help="only show events from this wiki, e.g. enwiki, hewiki, commonswiki")
    parser.add_argument("--type", help="only show this change type, e.g. edit, new, log, categorize")
    parser.add_argument("--no-bots", action="store_true", help="hide bot edits")
    parser.add_argument("--limit", type=int, default=0, help="stop after N matching events (0 = forever)")
    parser.add_argument("--raw", action="store_true", help="print the full JSON of each event")
    parser.add_argument("--duration", type=float, default=3, help="stop after N seconds (0 = forever, default: 3)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help=f"text file to write events to (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    url = BASE_URL + args.stream
    print(f"Connecting to {url} (Ctrl+C to stop)...", file=sys.stderr)

    with open(args.output, "w", encoding="utf-8") as out:
        count = listen(url, args, out)
    print(f"Wrote {count} events to {args.output}", file=sys.stderr)


def listen(url, args, out):
    """Stream matching events to stdout and `out`; return how many were written."""
    deadline = time.monotonic() + args.duration if args.duration else None
    last_id, count, backoff = None, 0, 1
    while True:
        try:
            for event_id, data in iter_sse_events(url, last_id):
                last_id = event_id or last_id
                backoff = 1
                try:
                    ev = json.loads(data)
                except json.JSONDecodeError:
                    continue
                if ev.get("meta", {}).get("domain") == "canary":
                    continue  # Wikimedia's synthetic health-check events
                if deadline and time.monotonic() >= deadline:
                    return count
                if not matches(ev, args):
                    continue

                if args.raw or args.stream != "recentchange":
                    line = json.dumps(ev, ensure_ascii=False, indent=2)
                else:
                    line = format_recentchange(ev)
                print(line, flush=True)
                out.write(line + "\n")

                count += 1
                if args.limit and count >= args.limit:
                    return count
        except KeyboardInterrupt:
            print(f"\nStopped after {count} events.", file=sys.stderr)
            return count
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as e:
            # The server closes connections periodically; resume from the last event we saw.
            print(f"Connection dropped ({e}); reconnecting in {backoff}s...", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    main()
