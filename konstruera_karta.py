import sys
import re
import base64
import zlib
import html
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd
from lxml import etree


# ---------- Hjälpfunktioner ----------


def _norm_col(s: str) -> str:
    """Normalisera kolumnnamn: case-insensitivt och robust mot å/ä/ö, mellanslag, / och -"""
    s = (s or "").strip().lower()
    s = s.replace("å", "a").replace("ä", "a").replace("ö", "o")
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" ", "").replace("/", "").replace("-", "")
    return s


def html_to_plain_text(s: str) -> str:
    """Avkoda HTML-entiteter och ta bort taggar, behåll rimliga radbrytningar."""
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</?(div|p|li|ul|ol|h[1-6])[^>]*>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    lines = [ln.strip() for ln in s.splitlines()]
    return "\n".join([ln for ln in lines if ln])


def load_drawio(
    path: str,
) -> Tuple[etree._Element, Optional[etree._Element], str, Optional[bytes]]:
    """
    Läs en .drawio/.xml.
    Returnerar (outer_root, diagram_node, mode, inner_xml_bytes_if_compressed)
      - mode: "uncompressed" om <mxGraphModel> redan finns direkt
              "compressed"   om <diagram> innehåller base64+deflate
    Om compressed returneras även original inner-bytes (kan ignoreras).
    """
    raw = Path(path).read_bytes()
    outer = etree.fromstring(raw)

    if outer.find(".//mxGraphModel") is not None:
        return outer, None, "uncompressed", None

    diagram = outer.find(".//diagram")
    if diagram is None or not (diagram.text or "").strip():
        raise ValueError("Hittade inget <diagram>-element med innehåll.")

    data = base64.b64decode(diagram.text.strip())

    # Försök båda decompress-varianterna
    inner_bytes = None
    try:
        inner_bytes = zlib.decompress(data, -zlib.MAX_WBITS)  # rå DEFLATE
    except zlib.error:
        inner_bytes = zlib.decompress(data)  # zlib-wrapped

    return outer, diagram, "compressed", inner_bytes


def parse_inner_root_for_mode(
    outer: etree._Element, mode: str, inner_bytes: Optional[bytes]
) -> etree._Element:
    """
    Ge ett element som innehåller mxGraphModel, oavsett mode.
    - uncompressed: outer redan innehåller mxGraphModel
    - compressed:   inner_root parsed från inner_bytes
    """
    if mode == "uncompressed":
        return outer
    else:
        return etree.fromstring(inner_bytes)


def save_drawio(
    outer: etree._Element,
    diagram: Optional[etree._Element],
    mode: str,
    inner_root: etree._Element,
    out_path: str,
):
    """
    Spara ändringarna tillbaka:
    - uncompressed: skriv ut outer
    - compressed:   serialisera inner_root, deflate (raw), base64-encoda och lägg i <diagram>
    """
    if mode == "uncompressed":
        xml = etree.tostring(
            outer, pretty_print=True, encoding="utf-8", xml_declaration=True
        )
        Path(out_path).write_bytes(xml)
        return

    # Compressed: byt ut innehållet i <diagram>
    inner_bytes = etree.tostring(inner_root, encoding="utf-8")
    # Använd rå DEFLATE (draw.io förväntar det i komprimerat läge)
    compressed = zlib.compress(inner_bytes)
    # För säkerhets skull: försök få rå deflate (utan zlib-header)
    try:
        # zlib.compress ger zlib-wrapped; vi gör raw-deflate med -MAX_WBITS via zlib library hack:
        # zlib.compressobj(wbits=-zlib.MAX_WBITS)
        compobj = zlib.compressobj(level=9, wbits=-zlib.MAX_WBITS)
        raw_deflate = compobj.compress(inner_bytes) + compobj.flush()
        payload = base64.b64encode(raw_deflate).decode("ascii")
    except Exception:
        payload = base64.b64encode(compressed).decode("ascii")

    diagram.text = payload
    xml = etree.tostring(
        outer, pretty_print=True, encoding="utf-8", xml_declaration=True
    )
    Path(out_path).write_bytes(xml)


def build_plats_map_from_excel(
    xlsx_path: str, sheet: Optional[str] = None
) -> Dict[str, List[Dict[str, str]]]:
    """
    Läs Excel och bygg en dict: platsnummer(str) -> lista av poster
    Poster innehåller 'båtnamn','Förnamn','Efternamn','plats' (som str).
    Filtrerar bort tomma plats-fält och extraherar siffra ur 'område/plats'.
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str)
    if isinstance(df, dict):
        df = next(iter(df.values()))

    # flexibel kolumnmatchning
    norm_to_orig = {_norm_col(c): c for c in df.columns}
    need = {
        "modell": None,
        "båtnamn": None,
        "förnamn": None,
        "efternamn": None,
        "område/plats": None,
    }
    for k in list(need.keys()):
        nk = _norm_col(k)
        if nk in norm_to_orig:
            need[k] = norm_to_orig[nk]
        else:
            # tolerera att man skrivit "omrade plats" etc
            if k == "område/plats":
                for alt in ("omradeplats", "omrade", "plats", "omrade_plats"):
                    if alt in norm_to_orig:
                        need[k] = norm_to_orig[alt]
                        break
    missing = [k for k, v in need.items() if v is None]
    if missing:
        raise KeyError(
            f"Saknar förväntade kolumner i Excel: {missing}. Hittade: {list(df.columns)}"
        )

    # Rensa whitespace
    for col in need.values():
        df[col] = df[col].astype(str).str.strip()

    def extract_platsnum_varvsomrade(text: str) -> str:
        """
        Returnera platsnumret ENDAST om cellen innehåller 'Varvsområde ... plats: <siffra>'.
        Ignorerar andra 'plats:'-träffar.
        Exempel som matchar:
        'Varvsområde 6 - plats: 633'  -> '633'
        '... Varvsområde: 3, plats 27 ...' -> '27'
        Exempel som INTE matchar:
        'Brygga D - plats: 202'
        'Båthusinnehav - plats: 8*4'
        """
        if not text:
            return None
        s = str(text)

        # Sök specifikt efter segment som innehåller "Varvsområde" följt av "plats: NNN"
        # Tål Å/å/Ä/ä/Ö/ö, valfria mellanrum, bindestreck, kolon.
        # Tar endast NUMERISKT platsnummer (ignorerar t.ex. '8*4').
        patterns = [
            r"(?i)varvsomr(?:å|a)de\s*\d*\s*[-,:]*\s*plats\s*[:\-]*\s*([0-9]+[A-Za-z]?)",
            r"(?i)varvsomr(?:å|a)de.*?plats\s*[:\-]*\s*([0-9]+[A-Za-z]?)",
        ]
        for pat in patterns:
            hits = re.findall(pat, s)
            if hits:
                # Om flera 'Varvsområde ... plats: NNN' i samma cell: ta sista (ofta mest relevant)
                return hits[-1].strip()

        return None

    plats_col = need["område/plats"]
    df["_plats"] = df[plats_col].apply(extract_platsnum_varvsomrade)

    # Filtrera bort tomma
    df = df[df["_plats"].notna() & (df["_plats"].str.strip() != "")]

    # Bygg map
    plats_map: Dict[str, List[Dict[str, str]]] = {}
    for _, row in df.iterrows():
        plats = str(row["_plats"]).strip()
        entry = {
            "modell": row[need["modell"]],
            "båtnamn": row[need["båtnamn"]],
            "Förnamn": row[need["förnamn"]],
            "Efternamn": row[need["efternamn"]],
            "plats": plats,
        }
        plats_map.setdefault(plats, []).append(entry)

    return plats_map


def make_value_text_for_entries(plats: str, entries: List[Dict[str, str]]) -> str:
    """
    Bygg strängen som ska in i draw.io-value.
    Om flera entries för samma plats: separera med " | "
    Ex: "14, Sally, Johan, Bergström | 14, Windy, Sara, Lindqvist"
    """
    parts = []
    for e in entries:
        parts.append(
            f'{plats}, Modell: {e.get("modell","")} Båt: {e.get("båtnamn","")}, Namn: {e.get("Förnamn","")} {e.get("Efternamn","")}'
        )
    return " | ".join(parts)


def set_cell_value(cell: etree._Element, new_value_text: str):
    """
    Sätt cellens value-attribut till HTML-escapad text (utan extra HTML-taggar).
    """
    cell.set("value", html.escape(new_value_text, quote=True))


# ---------- Huvudlogik ----------


def main():
    import argparse

    p = argparse.ArgumentParser(
        description="Uppdatera draw.io med namn från Excel baserat på platsnummer."
    )
    p.add_argument("drawio_in", help="Indata .drawio/.xml")
    p.add_argument(
        "excel",
        help="Excel-fil med kolumner: båtnamn, Förnamn, Efternamn, område/plats",
    )
    p.add_argument("drawio_out", help="Utdata .drawio")
    p.add_argument("--sheet", help="Bladnamn i Excel", default=None)
    args = p.parse_args()

    # 1) Läs Excel och bygg plats -> entries
    plats_map = build_plats_map_from_excel(args.excel, args.sheet)

    # 2) Läs draw.io
    outer, diagram, mode, inner_bytes = load_drawio(args.drawio_in)
    work_root = parse_inner_root_for_mode(outer, mode, inner_bytes)

    # 3) Gå igenom alla boxar (mxCell vertex="1") och ersätt value om texten är enbart en siffra som finns i Excel
    changed = 0
    found_plats = set()

    for cell in work_root.findall(".//mxCell"):
        if cell.get("vertex") != "1":
            continue
        raw_value = cell.get("value") or ""
        plain = html_to_plain_text(raw_value).strip()

        # Enbart siffra?
        if re.fullmatch(r"[0-9]+[\s\-]?[A-Za-z]?", plain):
            plats = plain.replace(" ", "").replace("-", "").lower()
            if plats in plats_map:
                new_text = make_value_text_for_entries(plats, plats_map[plats])
                set_cell_value(cell, new_text)
                changed += 1
                found_plats.add(plats)

    # 3b) Skriv ut platser som finns i Excel men inte i draw.io
    missing = sorted(set(plats_map.keys()) - found_plats)
    if missing:
        print("Platsnummer som finns i Excel men inte i kartan:")
        for p in missing:
            names = " | ".join(
                f"{e['båtnamn']} ({e['Förnamn']} {e['Efternamn']})"
                for e in plats_map[p]
            )
            print(f"  Plats {p}: {names}")
    else:
        print("Alla platser i Excel matchade boxar i kartan.")

    # 4) Spara tillbaka
    save_drawio(outer, diagram, mode, work_root, args.drawio_out)

    print(f"Klar. Uppdaterade {changed} box(ar). Skrev: {args.drawio_out}")


if __name__ == "__main__":
    main()
