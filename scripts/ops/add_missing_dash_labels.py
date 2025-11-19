#!/usr/bin/env python3
"""
add_missing_dash_labels.py

Post-process a DASH MPD from Shaka Packager and inject <Label> elements
ONLY for text (subtitle) AdaptationSets that do NOT already have one.

Usage:
    python add_missing_dash_labels.py input.mpd output.mpd

Notes:
    - We only touch AdaptationSets with contentType="text"
    - We require a known lang=... in LANG_LABELS
    - If a <Label> already exists, we leave it as-is
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# 🔤 Basic ISO 639-1 mapping (extend/replace with iso-639 library if available)
LANG_LABELS = {
    "aa": "Afar",
    "ab": "Abkhazian",
    "af": "Afrikaans",
    "ak": "Akan",
    "sq": "Albanian",
    "am": "Amharic",
    "ar": "Arabic",
    "an": "Aragonese",
    "hy": "Armenian",
    "as": "Assamese",
    "av": "Avaric",
    "ay": "Aymara",
    "az": "Azerbaijani",
    "ba": "Bashkir",
    "be": "Belarusian",
    "bn": "Bengali",
    "bi": "Bislama",
    "bs": "Bosnian",
    "br": "Breton",
    "bg": "Bulgarian",
    "my": "Burmese",
    "ca": "Catalan",
    "ch": "Chamorro",
    "ce": "Chechen",
    "ny": "Chichewa",
    "zh": "Chinese",
    "cv": "Chuvash",
    "kw": "Cornish",
    "co": "Corsican",
    "cr": "Cree",
    "hr": "Croatian",
    "cs": "Czech",
    "da": "Danish",
    "dv": "Divehi",
    "nl": "Dutch",
    "dz": "Dzongkha",
    "en": "English",
    "eo": "Esperanto",
    "et": "Estonian",
    "ee": "Ewe",
    "fo": "Faroese",
    "fj": "Fijian",
    "fi": "Finnish",
    "fr": "French",
    "ff": "Fula",
    "gl": "Galician",
    "ka": "Georgian",
    "de": "German",
    "el": "Greek",
    "gn": "Guarani",
    "gu": "Gujarati",
    "ht": "Haitian",
    "ha": "Hausa",
    "he": "Hebrew",
    "hi": "Hindi",
    "ho": "Hiri Motu",
    "hu": "Hungarian",
    "ia": "Interlingua",
    "id": "Indonesian",
    "ie": "Interlingue",
    "ga": "Irish",
    "ig": "Igbo",
    "ik": "Inupiaq",
    "io": "Ido",
    "is": "Icelandic",
    "it": "Italian",
    "iu": "Inuktitut",
    "ja": "Japanese",
    "jv": "Javanese",
    "kl": "Kalaallisut",
    "kn": "Kannada",
    "kr": "Kanuri",
    "ks": "Kashmiri",
    "kk": "Kazakh",
    "km": "Khmer",
    "ki": "Kikuyu",
    "rw": "Kinyarwanda",
    "ky": "Kyrgyz",
    "kv": "Komi",
    "kg": "Kongo",
    "ko": "Korean",
    "ku": "Kurdish",
    "kj": "Kwanyama",
    "la": "Latin",
    "lb": "Luxembourgish",
    "lg": "Ganda",
    "li": "Limburgish",
    "ln": "Lingala",
    "lo": "Lao",
    "lt": "Lithuanian",
    "lu": "Luba-Katanga",
    "lv": "Latvian",
    "gv": "Manx",
    "mk": "Macedonian",
    "mg": "Malagasy",
    "ms": "Malay",
    "ml": "Malayalam",
    "mt": "Maltese",
    "mi": "Maori",
    "mr": "Marathi",
    "mh": "Marshallese",
    "mn": "Mongolian",
    "na": "Nauru",
    "nv": "Navajo",
    "nd": "North Ndebele",
    "ne": "Nepali",
    "ng": "Ndonga",
    "nb": "Norwegian Bokmål",
    "nn": "Norwegian Nynorsk",
    "no": "Norwegian",
    "ii": "Nuosu",
    "nr": "South Ndebele",
    "oc": "Occitan",
    "oj": "Ojibwe",
    "cu": "Old Church Slavonic",
    "om": "Oromo",
    "or": "Oriya",
    "os": "Ossetian",
    "pa": "Punjabi",
    "pi": "Pali",
    "fa": "Persian",
    "pl": "Polish",
    "ps": "Pashto",
    "pt": "Portuguese",
    "qu": "Quechua",
    "rm": "Romansh",
    "rn": "Kirundi",
    "ro": "Romanian",
    "ru": "Russian",
    "sa": "Sanskrit",
    "sc": "Sardinian",
    "sd": "Sindhi",
    "se": "Northern Sami",
    "sm": "Samoan",
    "sg": "Sango",
    "sr": "Serbian",
    "gd": "Scottish Gaelic",
    "sn": "Shona",
    "si": "Sinhala",
    "sk": "Slovak",
    "sl": "Slovene",
    "so": "Somali",
    "st": "Southern Sotho",
    "es": "Spanish",
    "su": "Sundanese",
    "sw": "Swahili",
    "ss": "Swati",
    "sv": "Swedish",
    "ta": "Tamil",
    "te": "Telugu",
    "tg": "Tajik",
    "th": "Thai",
    "ti": "Tigrinya",
    "bo": "Tibetan",
    "tk": "Turkmen",
    "tl": "Tagalog",
    "tn": "Tswana",
    "to": "Tonga",
    "tr": "Turkish",
    "ts": "Tsonga",
    "tt": "Tatar",
    "tw": "Twi",
    "ty": "Tahitian",
    "ug": "Uyghur",
    "uk": "Ukrainian",
    "ur": "Urdu",
    "uz": "Uzbek",
    "ve": "Venda",
    "vi": "Vietnamese",
    "vo": "Volapük",
    "wa": "Walloon",
    "cy": "Welsh",
    "wo": "Wolof",
    "fy": "Western Frisian",
    "xh": "Xhosa",
    "yi": "Yiddish",
    "yo": "Yoruba",
    "za": "Zhuang",
    "zu": "Zulu",
}


def detect_namespace(root):
    """
    Detect the default XML namespace from the root tag, if present.
    Example root.tag: '{urn:mpeg:dash:schema:MPD:2011}MPD'
    """
    if root.tag.startswith("{") and "}" in root.tag:
        return root.tag[1 : root.tag.index("}")]
    return ""


def add_missing_labels(input_path: str, output_path: str) -> None:
    input_path = Path(input_path)
    output_path = Path(output_path)

    tree = ET.parse(input_path)
    root = tree.getroot()

    ns_uri = detect_namespace(root)
    if ns_uri:
        NS = {"mpd": ns_uri}
        ET.register_namespace("", ns_uri)  # preserve default namespace
        tag = lambda name: f"{{{ns_uri}}}{name}"
    else:
        NS = {}
        tag = lambda name: name

    # Find all AdaptationSets
    aset_xpath = ".//mpd:AdaptationSet" if ns_uri else ".//AdaptationSet"

    injected = 0
    skipped_existing = 0

    for aset in root.findall(aset_xpath, NS):
        lang = aset.get("lang")
        content_type = aset.get("contentType")

        # Only consider text AdaptationSets with known language mapping
        if content_type != "text":
            continue
        if not lang or lang not in LANG_LABELS:
            continue

        # Check if a <Label> already exists
        label_xpath = "mpd:Label" if ns_uri else "Label"
        existing_labels = aset.findall(label_xpath, NS)

        if existing_labels:
            skipped_existing += 1
            continue  # Do NOT overwrite existing labels

        # Inject new <Label> if none present
        label_el = ET.SubElement(aset, tag("Label"))
        label_el.text = LANG_LABELS[lang]
        injected += 1

    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"[OK] Wrote MPD to: {output_path}")
    print(f"  Injected new labels: {injected}")
    print(f"  AdaptationSets with existing labels left untouched: {skipped_existing}")


def main():
    if len(sys.argv) != 3:
        print("Usage: python add_missing_dash_labels.py input.mpd output.mpd", file=sys.stderr)
        sys.exit(1)

    input_mpd = sys.argv[1]
    output_mpd = sys.argv[2]
    add_missing_labels(input_mpd, output_mpd)


if __name__ == "__main__":
    main()
