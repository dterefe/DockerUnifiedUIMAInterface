#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import sys

import yaml
from tree_sitter import Language, Parser
import tree_sitter_java as tsjava


ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOTS = [
    ROOT / "duui-base" / "src" / "main" / "java",
    ROOT / "duui-core" / "src" / "main" / "java",
]
OUTPUT = ROOT / ".design" / "status_quo.yml"

TYPE_NODES = {
    "class_declaration": "class",
    "interface_declaration": "interface",
    "enum_declaration": "enum",
    "record_declaration": "record",
    "annotation_type_declaration": "annotation",
}
MEMBER_NODES = {
    "field_declaration",
    "constant_declaration",
    "method_declaration",
    "constructor_declaration",
    "compact_constructor_declaration",
    "annotation_type_element_declaration",
}


def text(src: bytes, node) -> str:
    return src[node.start_byte:node.end_byte].decode("utf-8")


def child_by_field(node, name: str):
    return node.child_by_field_name(name)


def named_children(node, *types: str):
    wanted = set(types)
    return [c for c in node.named_children if c.type in wanted]


def simple_name(src: bytes, node) -> str | None:
    n = child_by_field(node, "name")
    return text(src, n) if n is not None else None


def first_named(node, typ: str):
    for c in node.named_children:
        if c.type == typ:
            return c
    return None


def modifiers(src: bytes, node) -> dict:
    mod = first_named(node, "modifiers")
    out = {"keywords": [], "annotations": []}
    if mod is None:
        return out
    for c in mod.named_children:
        if c.type in {"marker_annotation", "annotation"}:
            out["annotations"].append(text(src, c))
        else:
            out["keywords"].append(text(src, c))
    return out


def strip_body_signature(src: bytes, node) -> str:
    body = child_by_field(node, "body")
    if body is None:
        return text(src, node).strip()
    return src[node.start_byte:body.start_byte].decode("utf-8").strip()


def type_parameters(src: bytes, node) -> list[str]:
    tp = child_by_field(node, "type_parameters")
    if tp is None:
        return []
    return [text(src, c) for c in tp.named_children if c.type == "type_parameter"]


def type_list(src: bytes, node) -> list[str]:
    if node is None:
        return []
    result = []
    for c in node.named_children:
        if c.type in {"type_list", "permits"}:
            result.extend(type_list(src, c))
        elif c.type not in {"modifiers"}:
            result.append(text(src, c))
    return result


def parameters(src: bytes, node) -> list[dict]:
    params = child_by_field(node, "parameters")
    if params is None:
        return []
    out = []
    for p in params.named_children:
        if p.type in {"formal_parameter", "spread_parameter"}:
            name = child_by_field(p, "name")
            typ = child_by_field(p, "type")
            out.append({
                "name": text(src, name) if name is not None else None,
                "type": text(src, typ) if typ is not None else None,
                "modifiers": modifiers(src, p),
                "signature": text(src, p),
            })
    return out


def enum_constants(src: bytes, node) -> list[dict]:
    body = child_by_field(node, "body")
    if body is None:
        return []
    out = []
    for c in body.named_children:
        if c.type == "enum_constant":
            out.append({
                "name": simple_name(src, c),
                "annotations": modifiers(src, c)["annotations"],
                "signature": text(src, c),
            })
    return out


def field_entries(src: bytes, node) -> list[dict]:
    decl_type = child_by_field(node, "type")
    base = {
        "type": text(src, decl_type) if decl_type is not None else None,
        "modifiers": modifiers(src, node),
        "signature": text(src, node).strip(),
    }
    declarators = named_children(node, "variable_declarator", "constant_declarator")
    if not declarators:
        return [{**base, "name": None}]
    out = []
    for d in declarators:
        name = child_by_field(d, "name")
        value = child_by_field(d, "value")
        out.append({
            **base,
            "name": text(src, name) if name is not None else None,
            "initializer": text(src, value) if value is not None else None,
        })
    return out


def callable_entry(src: bytes, node) -> dict:
    ret = child_by_field(node, "type")
    throws = first_named(node, "throws")
    return {
        "kind": {
            "method_declaration": "method",
            "constructor_declaration": "constructor",
            "compact_constructor_declaration": "compact_constructor",
            "annotation_type_element_declaration": "annotation_element",
        }[node.type],
        "name": simple_name(src, node),
        "return_type": text(src, ret) if ret is not None else None,
        "type_parameters": type_parameters(src, node),
        "parameters": parameters(src, node),
        "throws": type_list(src, throws),
        "modifiers": modifiers(src, node),
        "signature": strip_body_signature(src, node),
    }


def declared_interfaces(src: bytes, node) -> list[str]:
    interfaces = child_by_field(node, "interfaces")
    return type_list(src, interfaces)


def declared_superclass(src: bytes, node) -> str | None:
    superclass = child_by_field(node, "superclass")
    if superclass is None:
        return None
    vals = type_list(src, superclass)
    return vals[0] if vals else text(src, superclass)


def declared_permits(src: bytes, node) -> list[str]:
    permits = child_by_field(node, "permits")
    return type_list(src, permits)


def parse_type(src: bytes, node, package: str, parents: list[str]) -> dict:
    name = simple_name(src, node)
    qname = ".".join([package, *parents, name]) if package else ".".join([*parents, name])
    item = {
        "name": name,
        "qualified_name": qname,
        "kind": TYPE_NODES[node.type],
        "line": node.start_point[0] + 1,
        "modifiers": modifiers(src, node),
        "type_parameters": type_parameters(src, node),
        "superclass": declared_superclass(src, node),
        "interfaces": declared_interfaces(src, node),
        "permits": declared_permits(src, node),
        "subclasses": [],
        "enum_constants": enum_constants(src, node),
        "record_components": parameters(src, node) if node.type == "record_declaration" else [],
        "fields": [],
        "constructors": [],
        "methods": [],
        "nested_types": [],
        "signature": strip_body_signature(src, node),
    }
    body = child_by_field(node, "body")
    if body is not None:
        for c in body.named_children:
            if c.type in TYPE_NODES:
                item["nested_types"].append(parse_type(src, c, package, [*parents, name]))
            elif c.type in {"field_declaration", "constant_declaration"}:
                item["fields"].extend(field_entries(src, c))
            elif c.type in {
                "method_declaration",
                "constructor_declaration",
                "compact_constructor_declaration",
                "annotation_type_element_declaration",
            }:
                entry = callable_entry(src, c)
                if entry["kind"] in {"constructor", "compact_constructor"}:
                    item["constructors"].append(entry)
                else:
                    item["methods"].append(entry)
    return item


def package_name(src: bytes, tree) -> str:
    for c in tree.root_node.named_children:
        if c.type == "package_declaration":
            scoped = c.named_children[-1] if c.named_children else None
            return text(src, scoped) if scoped is not None else ""
    return ""


def imports(src: bytes, tree) -> list[str]:
    result = []
    for c in tree.root_node.named_children:
        if c.type == "import_declaration":
            result.append(text(src, c).strip())
    return result


def all_types(types: list[dict]):
    for t in types:
        yield t
        yield from all_types(t["nested_types"])


def simple_tail(type_name: str | None) -> str | None:
    if not type_name:
        return None
    return type_name.split("<", 1)[0].split("[", 1)[0].strip().split(".")[-1]


def main() -> int:
    lang = Language(tsjava.language())
    parser = Parser(lang)
    java_files = sorted(p for root in SOURCE_ROOTS for p in root.rglob("*.java"))
    data = {
        "status_quo": {
            "generated_from": [str(p.relative_to(ROOT)) for p in SOURCE_ROOTS],
            "java_file_count": len(java_files),
            "modules": {},
        }
    }
    type_index = {}
    simple_index = defaultdict(list)
    errors = []

    for path in java_files:
        src = path.read_bytes()
        tree = parser.parse(src)
        if tree.root_node.has_error:
            errors.append(f"parse_error: {path.relative_to(ROOT)}")
        package = package_name(src, tree)
        types = [
            parse_type(src, c, package, [])
            for c in tree.root_node.named_children
            if c.type in TYPE_NODES
        ]
        if not types:
            errors.append(f"no_type_declarations: {path.relative_to(ROOT)}")
        module = path.relative_to(ROOT).parts[0]
        mod = data["status_quo"]["modules"].setdefault(module, {"packages": {}})
        pkg = mod["packages"].setdefault(package, {"files": []})
        pkg["files"].append({
            "path": str(path.relative_to(ROOT)),
            "package": package,
            "imports": imports(src, tree),
            "types": types,
        })
        for t in all_types(types):
            type_index[t["qualified_name"]] = t
            simple_index[t["name"]].append(t)

    for t in list(type_index.values()):
        for parent in [t["superclass"], *t["interfaces"]]:
            tail = simple_tail(parent)
            if tail is None:
                continue
            for candidate in simple_index.get(tail, []):
                candidate["subclasses"].append(t["qualified_name"])

    for t in type_index.values():
        t["subclasses"] = sorted(set(t["subclasses"]))

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1

    OUTPUT.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=False, width=180),
        encoding="utf-8",
    )
    print(f"wrote {OUTPUT.relative_to(ROOT)} with {len(java_files)} files and {len(type_index)} type declarations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
