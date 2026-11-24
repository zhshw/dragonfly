"""Relink a separate span-instrumented binary without changing production sources or budgets."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
RAFT = "src/server/cluster/raft/"


def replace(source, before, after):
    if source.count(before) != 1:
        raise ValueError(f"Instrumentation anchor must occur exactly once: {before!r}")
    return source.replace(before, after)


def instrument(relative):
    source = (REPO / relative).read_text()
    edits = []
    if relative == RAFT + "runtime/group.cc":
        receive = re.search(
            r"(?m)^( +)auto s =\s+replication_\.ReceiveFetchResponse\(leader, "
            r"fetch->sequence, std::move\(response\),\s+&reset\);",
            source,
        )
        if receive is None:
            raise ValueError("Missing follower receive instrumentation anchor")
        indent = receive[1]
        edits = [
            (
                "      commit_changed_.wait_until(key.epoch(), deadline);",
                '      dfly_observe::Scope observed_commit("raft.await_commit");\n'
                "      commit_changed_.wait_until(key.epoch(), deadline);",
            ),
            (
                "      bool ran = worker_.Await([&] {",
                "      const auto observed_queue = dfly_observe::Now();\n"
                "      bool ran = worker_.Await([&] {\n"
                '        dfly_observe::Record("raft.worker.queue", observed_queue);\n'
                '        dfly_observe::Scope observed_worker("raft.worker.operation");',
            ),
            (
                "      io_changed_.wait(key.epoch());",
                '      dfly_observe::Scope observed_io("raft.await_io");\n'
                "      io_changed_.wait(key.epoch());",
            ),
            (
                "      if (auto s = apply_(*log_, *hw); !s.ok())",
                '      dfly_observe::Scope observed_apply("raft.apply", applied_, *hw);\n'
                "      if (auto s = apply_(*log_, *hw); !s.ok())",
            ),
            (
                "      ThisFiberSleep();\n    }\n    peer.Close();",
                '      dfly_observe::Scope observed_backoff("raft.peer.backoff", voter.key.id,\n'
                "                                            holds_alternative<Pull>(task));\n"
                "      ThisFiberSleep();\n    }\n    peer.Close();",
            ),
            (
                "    const auto start = steady_clock::now();\n    if (peer.Call(fetch->request, version, &response))\n      return false;",
                "    const auto start = steady_clock::now();\n"
                '    dfly_observe::Scope observed_rpc("raft.fetch.rpc", leader);\n'
                "    if (peer.Call(fetch->request, version, &response))\n      return false;\n"
                "    observed_rpc.Finish();",
            ),
            (
                receive[0],
                indent
                + 'dfly_observe::Scope observed_response("raft.fetch.receive", end.end_offset);\n'
                + receive[0]
                + "\n"
                + indent
                + "observed_response.Finish();",
            ),
        ]
    elif relative == RAFT + "runtime/data_node.cc":
        edits = [
            (
                "    if (owned_args) {",
                '    dfly_observe::Scope observed_input(owned_args ? "raft.set.input_take"\n'
                '                                                       : "raft.set.input_copy",\n'
                "                                       reserved_bytes);\n"
                "    if (owned_args) {",
            ),
            (
                "  const size_t reserved_bytes;",
                "  const size_t reserved_bytes;\n"
                "  const int64_t observed_input_start = dfly_observe::Now();",
            ),
            (
                "  void ReleaseInput() {",
                "  void ReleaseInput() {\n"
                '    dfly_observe::Record("raft.set.input_lifetime", observed_input_start,\n'
                "                         reserved_bytes, input.HeapMemory());",
            ),
            (
                "    string{}.swap(value);",
                "    string{}.swap(value);\n"
                '    dfly_observe::Record("raft.set.input_release", dfly_observe::Now(),\n'
                "                         input.HeapMemory(), reserved_bytes);",
            ),
            (
                "  ~AsyncPendingSet() override {",
                "  ~AsyncPendingSet() override {\n"
                '    dfly_observe::Record("raft.set.ticket_lifetime", observed_input_start,\n'
                "                         reserved_bytes, input.HeapMemory());",
            ),
            (
                "                replica->effects.RememberFetched(log, buffer, batches);",
                '                dfly_observe::Scope observed_cache("raft.fetch.cache_prepare", batches.size());\n'
                "                replica->effects.RememberFetched(log, buffer, batches);\n"
                "                observed_cache.Finish();\n"
                '                dfly_observe::Record("raft.fetch.cache_bytes", dfly_observe::Now(),\n'
                "                                     replica->effects.cached_bytes(), buffer->capacity());",
            ),
            (
                "        // Re-enter once per bounded batch, preserving log order and the completed prefix.\n"
                "        return owner_->Await([&] {",
                "        const auto observed_owner = dfly_observe::Now();\n"
                "        return owner_->Await([&] {\n"
                '          dfly_observe::Record("raft.apply.owner_queue", observed_owner, effects.size());\n'
                '          dfly_observe::Scope observed_batch("raft.apply.owner_batch", effects.size());',
            ),
            (
                "    request->proposal.appended.Wait();",
                '    dfly_observe::Scope observed_appended("raft.set.await_append");\n'
                "    request->proposal.appended.Wait();\n    observed_appended.Finish();",
            ),
            (
                "  if (proposal.stored)\n    *position",
                '  dfly_observe::Scope observed_applied("raft.set.await_apply", proposal.end.end_offset);\n'
                "  if (proposal.stored)\n    *position",
            ),
            (
                "    local->queue_changed.wait_until(key.epoch(), steady_clock::now() + milliseconds(100));",
                '    dfly_observe::Scope observed_pressure("raft.set.backpressure");\n'
                "    local->queue_changed.wait_until(key.epoch(), steady_clock::now() + milliseconds(100));",
            ),
            (
                "  request->appended.Wait();",
                '  dfly_observe::Scope observed_appended("raft.set.sync_await_append");\n'
                "  request->appended.Wait();",
            ),
            (
                "  size_t records_size = 0;\n  for (size_t i = 0; i < batch.requests.size(); ++i)",
                '  dfly_observe::Scope observed_encode("raft.set.encode", batch.requests.size(), batch.value_bytes);\n'
                "  size_t records_size = 0;\n  for (size_t i = 0; i < batch.requests.size(); ++i)",
            ),
            (
                "  bool may_have_appended = false;",
                "  observed_encode.Finish();\n"
                '  dfly_observe::Scope observed_submit("raft.set.submit", batch.requests.size(), records.size());\n'
                "  bool may_have_appended = false;",
            ),
            (
                "  return absl::OkStatus();\n}\n\nabsl::StatusOr<DataReplicationState> DataNodeImpl::ReplicationState()",
                '  dfly_observe::Record("raft.set.cache_bytes", dfly_observe::Now(),\n'
                "                       local.effects.cached_bytes(), buffer->capacity());\n"
                "  return absl::OkStatus();\n}\n\nabsl::StatusOr<DataReplicationState> DataNodeImpl::ReplicationState()",
            ),
        ]
    elif relative == RAFT + "data/set_effect.cc":
        edits = [
            (
                "      next = std::move(pending_.front());",
                '      dfly_observe::Record("raft.apply.cache_hit", dfly_observe::Now(), next_offset);\n'
                "      next = std::move(pending_.front());",
            ),
            (
                "      auto bytes = log.Read(next_offset, hw, 1, &base);",
                '      dfly_observe::Scope observed_read("raft.apply.read");\n'
                "      auto bytes = log.Read(next_offset, hw, 1, &base);\n"
                "      observed_read.Finish();",
            ),
            (
                "      auto batch = DecodeRecordBatch(*bytes, &records);",
                '      dfly_observe::Scope observed_batch("raft.apply.decode_batch", bytes->size());\n'
                "      auto batch = DecodeRecordBatch(*bytes, &records);\n"
                "      observed_batch.Finish();",
            ),
            (
                "      auto effect = DecodeSetEffect(*records.front().value);",
                '      dfly_observe::Scope observed_effect("raft.apply.decode_effect");\n'
                "      auto effect = DecodeSetEffect(*records.front().value);\n"
                "      observed_effect.Finish();",
            ),
        ]
    elif relative == RAFT + "log/partition_log.cc":
        edits = [
            (
                '  if (fsync(log_fd_) != 0)\n    return io_failure_ = DiskError("Syncing buffered quorum log");',
                '  dfly_observe::Scope observed_flush("raft.log.sync_flush", buffered_bytes_);\n'
                '  if (fsync(log_fd_) != 0)\n    return io_failure_ = DiskError("Syncing buffered quorum log");',
            ),
            (
                "  vector<Batch> pending;\n  bool memory",
                '  dfly_observe::Scope observed_append("raft.log.append", bytes.size(), follower);\n'
                "  vector<Batch> pending;\n  bool memory",
            ),
            (
                "  io_failure_ = WriteAt(log_fd_, active_bytes_, bytes.substr(0, length));",
                '  dfly_observe::Scope observed_write("raft.log.pwrite", length);\n'
                "  io_failure_ = WriteAt(log_fd_, active_bytes_, bytes.substr(0, length));\n"
                "  observed_write.Finish();",
            ),
            (
                "  auto pending = make_unique<PendingAppend>();\n  auto end = End();",
                '  dfly_observe::Scope observed_decode("raft.log.async_prepare", bytes->size());\n'
                "  auto pending = make_unique<PendingAppend>();\n  auto end = End();",
            ),
        ]
    elif relative == RAFT + "storage/folly_io.cc":
        edits = [
            (
                "    auto* event_base = thread_.getEventBase();",
                "    const auto observed_submit = dfly_observe::Now();\n"
                "    auto* event_base = thread_.getEventBase();",
            ),
            (
                "[event_base, fd, offset, bytes, done = std::move(done)]() mutable {",
                "[event_base, fd, offset, bytes, observed_submit, done = std::move(done)]() mutable {\n"
                '          dfly_observe::Record("raft.folly.queue", observed_submit, fd, bytes.size());',
            ),
            (
                "          auto completion = [event_base, done = std::move(done)](int result) mutable {",
                "          const auto observed_issue = dfly_observe::Now();\n"
                "          auto completion = [event_base, fd, offset, observed_issue, done = std::move(done)](int result) mutable {\n"
                '            dfly_observe::Record(offset < 0 ? "raft.folly.fsync" : "raft.folly.write",\n'
                "                                 observed_issue, fd, result);",
            ),
        ]
    elif relative == "src/server/journal/streamer.cc":
        edits = [
            (
                "void ReplicaStreamer::ConsumeJournalChange(const JournalChangeItem& item) {",
                "void ReplicaStreamer::ConsumeJournalChange(const JournalChangeItem& item) {\n"
                '  dfly_observe::Scope observed_journal("cluster.journal.enqueue", item.journal_item.data.size());',
            )
        ]
    elif relative == "src/server/journal/buffered_socket_writer.cc":
        edits = [
            (
                "  ++throttle_count_;",
                '  dfly_observe::Scope observed_throttle("cluster.journal.throttle");\n'
                "  ++throttle_count_;",
            ),
            (
                "[this, len = in_flight_bytes_](std::error_code ec) { OnCompletion(ec, len); }",
                "[this, len = in_flight_bytes_, observed_start = dfly_observe::Now()](std::error_code ec) {\n"
                '                      dfly_observe::Record("cluster.journal.send", observed_start, len, ec.value());\n'
                "                      OnCompletion(ec, len);\n                    }",
            ),
        ]
    elif relative == "src/server/journal/executor.cc":
        edits = [
            (
                "facade::DispatchResult JournalExecutor::Execute(facade::ParsedArgs args, CommandContext* cmd_cntx) {",
                "facade::DispatchResult JournalExecutor::Execute(facade::ParsedArgs args, CommandContext* cmd_cntx) {\n"
                '  dfly_observe::Scope observed_execute("journal.apply");',
            )
        ]
    if not edits:
        raise ValueError(relative)
    for before, after in edits:
        if relative == RAFT + "runtime/group.cc":
            after = re.sub(
                r'"(raft\.[a-z_.]+)"',
                lambda m: f'(kind_ == Kind::kData ? "{m[1]}" : "metadata.{m[1][5:]}")',
                after,
            )
        source = replace(source, before, after)
    return '#include "span_trace.h"\n' + source


def prepare(build, output, skip_build=False):
    output.mkdir(parents=True, exist_ok=False)
    if not skip_build:
        subprocess.run(["ninja", "-C", build, "-j4", "dragonfly", "raft_storage"], check=True)
    commands = json.loads((build / "compile_commands.json").read_text())
    units = [
        RAFT + "runtime/group.cc",
        RAFT + "runtime/data_node.cc",
        RAFT + "data/set_effect.cc",
        RAFT + "log/partition_log.cc",
        RAFT + "storage/folly_io.cc",
        "src/server/journal/streamer.cc",
        "src/server/journal/buffered_socket_writer.cc",
        "src/server/journal/executor.cc",
    ]
    hashes = {}
    for relative in units:
        unit = REPO / relative
        patched = output / unit.name
        patched.write_text(instrument(relative))
        hashes[relative] = hashlib.sha256(unit.read_bytes()).hexdigest()
        (entry,) = (c for c in commands if Path(c["directory"], c["file"]).resolve() == unit)
        args = entry.get("arguments") or shlex.split(entry["command"])
        args[args.index("-c") + 1] = str(patched)
        args[args.index("-o") + 1] = str(patched.with_suffix(".o"))
        subprocess.run(
            [*args, "-iquote", str(unit.parent), "-iquote", str(HERE)],
            cwd=entry["directory"],
            check=True,
        )
    lines = subprocess.check_output(
        ["ninja", "-t", "commands", "dragonfly"], cwd=build, text=True
    ).splitlines()
    link = shlex.split(lines[-1])
    if link[:2] == [":", "&&"]:
        link = link[2:]
    if "&&" in link:
        link = link[: link.index("&&")]
    (direct,) = (a for a in link if a.endswith("/runtime/data_node.cc.o"))
    link[link.index(direct)] = str(output / "data_node.o")
    output_index = link.index("-o")
    link[output_index + 1] = str(output / "dragonfly")
    for unit in units:
        if not unit.endswith("/data_node.cc"):
            link.insert(output_index, str(output / Path(unit).with_suffix(".o").name))
    subprocess.run(link, cwd=build, check=True)
    (output / "raft_storage").symlink_to(build / "raft_storage")
    for label, path in {"baseline": build / "dragonfly", "probe": output / "dragonfly"}.items():
        hashes[label] = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
    (output / "sources.json").write_text(json.dumps(hashes, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--skip-build", action="store_true", help="Use an already built baseline")
    args = parser.parse_args()
    prepare(args.build_dir.resolve(), args.output.resolve(), args.skip_build)
