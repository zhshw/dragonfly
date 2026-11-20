"""Generate the flexible KRaft Vote codec from the Kafka checkout's message schemas.

Usage: python3 generate.py /path/to/kafka
Run the repository clang-format hook on vote.h and vote.cc after generation.
"""

import argparse
import json
import re
from pathlib import Path

NOTICE = """// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.
"""
NAMESPACE = "dfly::cluster::raft::protocol"
TYPES = {
    "bool": "bool",
    "int16": "int16_t",
    "int32": "int32_t",
    "int64": "int64_t",
    "uint16": "uint16_t",
    "uuid": "Uuid",
    "string": "std::string",
}


def first_version(versions):
    match = re.fullmatch(r"(\d+)\+", versions)
    if not match:
        raise ValueError(f"Unsupported version range {versions}")
    return int(match[1])


def field_name(field):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", field["name"]).lower()


def cpp_field(field):
    raw = field["type"]
    if raw.startswith("[]"):
        if not field.get("fields"):
            raise ValueError(f"Expected an array of structures: {field}")
        return f"std::vector<{raw[2:]}>"
    if "nullableVersions" in field:
        if raw != "string" or field["nullableVersions"] != field["versions"]:
            raise ValueError(f"Unsupported nullability: {field}")
        return "std::optional<std::string>"
    return TYPES[raw]


def emit_struct(name, fields, constants, header, impl, parents=()):
    header.append(f"struct {name} {{")
    header.extend(constants)
    visitor = []
    tags = ["m.unknown_tags"]
    for field in fields:
        since = first_version(field["versions"])
        member = "m." + field_name(field)
        if "fields" in field:
            emit_struct(field["type"][2:], field["fields"], [], header, impl, (*parents, name))
        default = field.get("default", "null")
        initializer = "{}" if default == "null" else f" = {int(default)}"
        header.append(f"{cpp_field(field)} {field_name(field)}{initializer};")
        if "tag" in field:
            if field["taggedVersions"] != field["versions"] or not field["type"].startswith("[]"):
                raise ValueError(f"Unsupported tagged field shape: {field}")
            tags.append(f"Tagged{{{field['tag']}, {since}, {member}}}")
        elif since:
            visitor.append(f"if (io.version >= {since}) {{")
            visitor.append(f"if (!io.Field({member})) return false;")
            if not field.get("ignorable", False):
                if default not in ("null", "0", "false"):
                    raise ValueError(f"Unsupported versioned default: {field}")
                visitor.append(f"}} else if (!io.Absent({member})) {{ return false;")
            visitor.append("}")
        else:
            visitor.append(f"if (!io.Field({member})) return false;")
    header.extend(
        ["TaggedFields unknown_tags;", f"bool operator==(const {name}&) const = default;", "};", ""]
    )
    qualified = "::".join((*parents, name))
    impl.extend(
        [
            f"template <> struct Schema<{qualified}> {{",
            "template <typename IO, typename T> static bool Visit(IO& io, T& m) {",
            *visitor,
            f"return io.Tags({', '.join(tags)});",
            "}",
            "};",
            "",
        ]
    )


def generate(schema_dir):
    header = [
        NOTICE,
        "#pragma once",
        "",
        "#include <optional>",
        "#include <string_view>",
        "#include <vector>",
        "",
        '#include "server/cluster/raft/protocol/wire.h"',
        "",
        f"namespace {NAMESPACE} {{",
        "",
    ]
    impl = [
        NOTICE,
        '#include "server/cluster/raft/protocol/vote.h"',
        "",
        '#include "server/cluster/raft/protocol/flexible_message.h"',
        "",
        f"namespace {NAMESPACE} {{",
        "namespace detail {",
        "",
    ]
    functions = []
    for name in ("VoteRequest", "VoteResponse"):
        schema = json.loads(
            re.sub(
                r"^\s*//[^\n]*", "", (schema_dir / f"{name}.json").read_text(), flags=re.MULTILINE
            )
        )
        if schema["flexibleVersions"] != "0+" or set(schema["headerVersions"]) != {"0+"}:
            raise ValueError("Expected a flexible message with a fixed header version")
        low, high = map(int, schema["validVersions"].split("-"))
        constants = [
            f"static constexpr int16_t kApiKey = {schema['apiKey']};",
            f"static constexpr int16_t kMinVersion = {low};",
            f"static constexpr int16_t kMaxVersion = {high};",
            f"static constexpr int16_t kHeaderVersion = {schema['headerVersions']['0+']};",
            "",
        ]
        emit_struct(name, schema["fields"], constants, header, impl)
        header.append(
            "// Encode or decode one complete body without the RPC header or frame length."
        )
        header.append("// On failure the destination keeps its previous value.")
        for function, signature, args in [
            (
                "Encode",
                f"const {name}& message, int16_t version, std::string* out",
                "message, version, out",
            ),
            (
                "Decode",
                f"std::string_view bytes, int16_t version, {name}* out",
                "bytes, version, out",
            ),
        ]:
            decl = f"CodecError {function}({signature})"
            header.append(decl + ";")
            functions.extend([decl + " {", f"return detail::{function}Message({args});", "}", ""])
        header.append("")
    impl.extend(["}  // namespace detail", "", *functions])
    for filename, lines in [("vote.h", header), ("vote.cc", impl)]:
        lines.extend(["", f"}}  // namespace {NAMESPACE}", ""])
        Path(__file__).with_name(filename).write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kafka", type=Path)
    generate(parser.parse_args().kafka / "clients/src/main/resources/common/message")
