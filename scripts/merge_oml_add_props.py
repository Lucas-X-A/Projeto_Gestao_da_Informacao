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


INSTANCE_RE = re.compile(r"^\s*instance\s+(?P<id>[^\s]+)\s*:\s*(?P<type>[^\s]+)\s*\[", re.MULTILINE)


def extract_instances(text: str) -> Dict[str, Tuple[int, int, str, str]]:
    """Retorna mapa id -> (start_idx, end_idx, type, block_text)"""
    instances = {}
    pos = 0
    while True:
        idx = text.find("instance ", pos)
        if idx == -1:
            break
        if idx > 0 and text[idx-1] not in (' ', '\t', '\n', '\r'):
            pos = idx + 1
            continue

        rest = text[idx+9:]
        m = re.match(r"([^\s:]+)\s*:\s*([^\s\[]+)\s*\[", rest)
        if not m:
            pos = idx + 1
            continue

        inst_id = m.group(1)
        inst_type = m.group(2)

        block_start = idx
        block_end = idx + 9 + m.end()
        close_pos = text.find("\n\t]", block_end)
        if close_pos == -1:
            pos = idx + 1
            continue

        block_end = close_pos + 3
        block_text = text[block_start:block_end]

        instances[inst_id] = (block_start, block_end, inst_type, block_text)
        pos = block_end

    return instances


def find_property_line(block: str, prop: str) -> str | None:
    """Procura uma propriedade no bloco sem regex"""
    for line in block.split('\n'):
        stripped = line.strip()
        if stripped.startswith(prop + ' '):
            return line
    return None


def replace_property_line(block: str, prop: str, new_value: str) -> str:
    """Substitui propriedade preservando indentação"""
    lines = []
    for line in block.split('\n'):
        stripped = line.strip()
        if stripped.startswith(prop + ' '):
            # Preservar indentação da linha original
            indent = line[:len(line) - len(stripped)]
            lines.append(f"{indent}{new_value}")
        else:
            lines.append(line)
    return '\n'.join(lines)


def insert_property_into_block(block: str, new_prop: str) -> str:
    """Insere propriedade antes do "]" final"""
    lines = block.rstrip('\n').split('\n')
    if lines[-1].strip() == ']':
        indent = lines[-1][:len(lines[-1]) - len(lines[-1].lstrip())]
        lines.insert(-1, f"{indent}\t{new_prop}")
        return '\n'.join(lines)
    return block


def insert_or_replace_property(block: str, prop_line: str) -> str:
    """Insere ou substitui propriedade"""
    prop = prop_line.split()[0]
    if find_property_line(block, prop):
        return replace_property_line(block, prop, prop_line)
    return insert_property_into_block(block, prop_line)



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

    # Helpers para decidir tipos
    def is_discente(t: str) -> bool:
        return "Discente" in t

    def is_producao(t: str) -> bool:
        return "Produc" in t or "Producao" in t

    # Mapa de instâncias atualizadas
    inst_updates: Dict[str, str] = {}

    for iid, (old_s, old_e, old_type, old_block) in old_insts.items():
        updated_block = old_block

        if iid in new_insts:
            _n_s, _n_e, new_type, new_block = new_insts[iid]

            for prop in props_to_copy:
                # Filtrar por tipo antes de copiar
                if prop == "cti:an_titulacao" and not (is_discente(old_type) or is_discente(new_type)):
                    continue
                if prop == "cti:an_base_producao" and not (is_producao(old_type) or is_producao(new_type)):
                    continue

                for line in new_block.split('\n'):
                    stripped = line.strip()
                    if stripped.startswith(prop + ' '):
                        updated_block = insert_or_replace_property(updated_block, stripped)
                        break

        inst_updates[iid] = updated_block

    # Reconstruir arquivo de old_text, substituindo blocos (sem anexar novas instâncias)
    out_text = old_text
    for iid in sorted(old_insts.keys(), key=lambda i: old_insts[i][0], reverse=True):
        old_s, old_e, _old_type, _block = old_insts[iid]
        updated_block = inst_updates[iid]
        out_text = out_text[:old_s] + updated_block + out_text[old_e:]

    out_path = Path(args.out) if args.out else old_path.with_suffix(old_path.suffix + ".merged")
    out_path.write_text(out_text, encoding="utf-8")
    print(f"Arquivo de saída escrito: {out_path}")



if __name__ == "__main__":
    main()
