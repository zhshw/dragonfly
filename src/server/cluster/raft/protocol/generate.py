"""Generate KRaft message codecs from the Kafka checkout's message schemas.

Usage: python3 generate.py /path/to/kafka
Run the repository clang-format hook on the generated files after generation.
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


def version_condition(versions):
    if versions.endswith("+"):
        since = first_version(versions)
        return f"io.version >= {since}" if since else ""
    if "-" in versions:
        low, high = map(int, versions.split("-"))
        return f"io.version >= {low} && io.version <= {high}" if low else f"io.version <= {high}"
    return f"io.version == {int(versions)}"


def field_name(field):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", field["name"]).lower()


def cpp_field(field):
    raw = field["type"]
    if raw.startswith("[]"):
        element = raw[2:] if field.get("fields") else TYPES[raw[2:]]
        return f"std::vector<{element}>"
    if "nullableVersions" in field:
        if raw != "string" or field["nullableVersions"] != field["versions"]:
            raise ValueError(f"Unsupported nullability: {field}")
        return "std::optional<std::string>"
    return TYPES[raw]


def emit_struct(name, fields, constants, header, impl, flexible, parents=()):
    header.append(f"struct {name} {{")
    header.extend(constants)
    visitor = []
    tags = ["m.unknown_tags"]
    for field in fields:
        condition = version_condition(field["versions"])
        member = "m." + field_name(field)
        if "fields" in field:
            emit_struct(
                field["type"][2:], field["fields"], [], header, impl, flexible, (*parents, name)
            )
        default = field.get("default", "null")
        if field.get("flexibleVersions"):
            raise ValueError(f"Per-field flexible versions require an explicit codec: {field}")
        initializer = "{}" if default in ("null", "false") else f" = {int(default)}"
        header.append(f"{cpp_field(field)} {field_name(field)}{initializer};")
        if "tag" in field:
            since = first_version(field["taggedVersions"])
            if field["taggedVersions"] != field["versions"] or since < flexible:
                raise ValueError(f"Unsupported tagged field shape: {field}")
            if field["type"].startswith("[]"):
                present = f"!{member}.empty()"
            elif field["type"] == "bool":
                present = member
            elif field["type"] in ("int16", "int32", "int64"):
                present = f"{member} != {int(default)}"
            else:
                raise ValueError(f"Unsupported tagged field type: {field}")
            tags.append(f"Tagged{{{field['tag']}, {since}, {member}, {present}}}")
        elif condition:
            visitor.append(f"if ({condition}) {{")
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
            *(
                [f"if (io.version < {flexible}) return io.Absent(m.unknown_tags);"]
                if flexible
                else []
            ),
            f"return io.Tags({', '.join(tags)});",
            "}",
            "};",
            "",
        ]
    )


def generate(schema_dir, api, stem):
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
        f'#include "server/cluster/raft/protocol/{stem}.h"',
        "",
        '#include "server/cluster/raft/protocol/flexible_message.h"',
        "",
        f"namespace {NAMESPACE} {{",
        "namespace detail {",
        "",
    ]
    functions = []
    for name in (api + "Request", api + "Response"):
        schema = json.loads(
            re.sub(
                r"^\s*//[^\n]*", "", (schema_dir / f"{name}.json").read_text(), flags=re.MULTILINE
            )
        )
        flexible = first_version(schema["flexibleVersions"])
        headers = schema["headerVersions"]
        low, high = map(int, schema["validVersions"].split("-"))
        constants = [
            f"static constexpr int16_t kApiKey = {schema['apiKey']};",
            f"static constexpr int16_t kMinVersion = {low};",
            f"static constexpr int16_t kMaxVersion = {high};",
            f"static constexpr int16_t kFirstFlexibleVersion = {flexible};",
        ]
        legacy_range = "0" if flexible == 1 else f"0-{flexible - 1}"
        if set(headers) == {"0+"}:
            constants.append(f"static constexpr int16_t kHeaderVersion = {headers['0+']};")
            constants.append(
                "static constexpr int16_t HeaderVersion(int16_t) {return kHeaderVersion;}"
            )
        elif set(headers) == {legacy_range, f"{flexible}+"}:
            legacy_header = headers[legacy_range]
            flexible_header = headers[f"{flexible}+"]
            constants.append(
                f"static constexpr int16_t HeaderVersion(int16_t version) {{"
                f"return version < kFirstFlexibleVersion ? {legacy_header} : {flexible_header}; }}"
            )
        else:
            raise ValueError(f"Unsupported header version map {headers}")
        constants.append("")
        emit_struct(name, schema["fields"], constants, header, impl, flexible)
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
    for filename, lines in [(stem + ".h", header), (stem + ".cc", impl)]:
        lines.extend(["", f"}}  // namespace {NAMESPACE}", ""])
        Path(__file__).with_name(filename).write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kafka", type=Path)
    schema_dir = parser.parse_args().kafka / "clients/src/main/resources/common/message"
    generate(schema_dir, "Vote", "vote")
    generate(schema_dir, "ApiVersions", "api_versions")
    generate(schema_dir, "BeginQuorumEpoch", "begin_epoch")
    generate(schema_dir, "EndQuorumEpoch", "end_epoch")
