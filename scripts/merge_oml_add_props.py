#!/usr/bin/env python3
"""Mescla não-destrutiva de propriedades OML.

Copiar apenas propriedades específicas (ex: cti:an_base_producao, cti:an_titulacao)
do OML gerado pelo pipeline (CAPES) para o OML existente que já contém dados
de enriquecimento (Scopus), sem remover outras linhas.

Uso:
  python scripts/merge_oml_add_props.py --new new_cti-pe.oml --old cti-pe.oml --out merged_cti-pe.oml

Se --out não for informado, sobrescreve o arquivo antigo após criar backup.
"""
import argparse
import re
from pathlib import Path
from typing import Dict, Tuple


INSTANCE_RE = re.compile(r"^\s*instance\s+(?P<id>[^\s]+)\s*:\s*(?P<type>[^\s]+)\s*\[")


def extract_instances(text: str) -> Dict[str, Tuple[int, int, str]]:
    """Retorna mapa id -> (start_idx, end_idx, block_text)"""
    instances = {}
    for m in INSTANCE_RE.finditer(text):
        start = m.start()
        # find closing pattern '\n\t]' after start
        close_match = re.search(r"\n\t\]", text[start:])
        if not close_match:
            continue
        end = start + close_match.end()
        block = text[start:end]
        inst_id = m.group("id")
        instances[inst_id] = (start, end, block)
    return instances


def has_property(block: str, prop: str) -> bool:
    return prop in block


def insert_property_into_block(block: str, prop_line: str) -> str:
    # insert before the closing '\n\t]'
    return re.sub(r"\n\t\]$", f"\n\t\t{prop_line}\n\t]", block)


def main():
    parser = argparse.ArgumentParser(description="Mescla propriedades de OML (não-destrutivo)")
    parser.add_argument("--new", required=True, help="OML recém-gerado (CAPES)")
    parser.add_argument("--old", required=True, help="OML existente (com Scopus)")
    parser.add_argument("--out", default=None, help="Arquivo de saída (se omitido, sobrescreve --old após backup)")
    args = parser.parse_args()

    new_path = Path(args.new)
    old_path = Path(args.old)
    if not new_path.exists() or not old_path.exists():
        raise SystemExit("Arquivo --new ou --old não encontrado")

    new_text = new_path.read_text(encoding="utf-8")
    old_text = old_path.read_text(encoding="utf-8")

    new_insts = extract_instances(new_text)
    old_insts = extract_instances(old_text)

    props_to_copy = ["cti:an_base_producao", "cti:an_titulacao"]

    out_text = old_text
    appended = []

    for iid, (_s, _e, new_block) in new_insts.items():
        if iid in old_insts:
            # check each property: if present in new but missing in old, insert
            _, _, old_block = old_insts[iid]
            new_props = [line.strip() for line in new_block.splitlines() if any(p in line for p in props_to_copy)]
            for line in new_props:
                prop = None
                for p in props_to_copy:
                    if p in line:
                        prop = p
                        break
                if prop and not has_property(old_block, prop):
                    # insert this property into the old_text instance block
                    # we must find the exact block in out_text (it may shift as we modify)
                    inst_pattern = re.compile(r"(^\s*instance\s+" + re.escape(iid) + r"\s*:\s*[^\n]+\[[\s\S]*?\n\t\])", re.MULTILINE)
                    m = inst_pattern.search(out_text)
                    if m:
                        block = m.group(1)
                        new_block_text = insert_property_into_block(block, line)
                        out_text = out_text[: m.start(1)] + new_block_text + out_text[m.end(1) :]
        else:
            # instance not present in old -> append whole block before final closing '}'
            appended.append(new_block)

    if appended:
        # insert before final '\n}'
        out_text = re.sub(r"\n\}$", "\n" + "\n".join(appended) + "\n}", out_text)

    out_path = Path(args.out) if args.out else old_path.with_suffix(old_path.suffix + ".merged")
    out_path.write_text(out_text, encoding="utf-8")
    print(f"Arquivo de saída escrito: {out_path}")


if __name__ == "__main__":
    main()
