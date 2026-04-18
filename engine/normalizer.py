"""
Text Normalizer for UBID Entity Resolution.

Handles Indian business name and address normalization:
- Case normalization
- Legal suffix removal/standardization
- Common abbreviation expansion
- Address normalization (Indian address patterns)
- Whitespace/punctuation cleanup
"""

import re
from metaphone import doublemetaphone


# ── Legal suffixes to strip for comparison ──
LEGAL_SUFFIXES = [
    r'\bprivate\s+limited\b', r'\bpvt\.?\s*ltd\.?\b', r'\bp\.?\s*ltd\.?\b',
    r'\blimited\b', r'\bltd\.?\b', r'\bllp\b', r'\bllc\b',
    r'\bincorporated\b', r'\binc\.?\b', r'\bcorporation\b', r'\bcorp\.?\b',
    r'\b\(regd\.?\)\b', r'\(registered\)', r'\bregd\.?\b',
    r'\b\(unit[\s-]*\d*\)\b', r'\bunit[\s-]*\d+\b',
]

# ── Domain-specific string block-list ──
INDIAN_STOPWORDS = [
    r'\benterprises\b', r'\btraders\b', r'\bindustries\b', r'\bworks\b',
    r'\bagency\b', r'\bagencies\b', r'\bassociates\b', r'\bco\b', r'\bcompany\b'
]

# ── Business abbreviation mappings ──
BIZ_ABBREVIATIONS = {
    r'\bmfg\b': 'manufacturing',
    r'\bmnfg\b': 'manufacturing',
    r'\bengg\b': 'engineering',
    r'\beng\b': 'engineering',
    r'\bind\b': 'industries',
    r'\binds\b': 'industries',
    r'\bentrp\b': 'enterprises',
    r'\bentpr\b': 'enterprises',
    r'\benterprises\b': 'enterprises',
    r'\btrdrs\b': 'traders',
    r'\bassoc\b': 'associates',
    r'\btech\b': 'technology',
    r'\bintl\b': 'international',
    r'\bnatl\b': 'national',
    r'\bpharm\b': 'pharmaceuticals',
    r'\bchem\b': 'chemicals',
    r'\belec\b': 'electronics',
    r'\bauto\b': 'automobile',
    r'\bsvc\b': 'services',
    r'\bsvcs\b': 'services',
    r'\bsoln\b': 'solutions',
    r'\bsolns\b': 'solutions',
    r'\bmfrs\b': 'manufacturers',
    r'\bprod\b': 'products',
}

# ── Address abbreviation mappings ──
ADDR_ABBREVIATIONS = {
    r'\brd\b': 'road',
    r'\bst\b': 'street',
    r'\bmn\b': 'main',
    r'\bcr\b': 'cross',
    r'\bblk\b': 'block',
    r'\bflr\b': 'floor',
    r'\bbldg\b': 'building',
    r'\bopp\b': 'opposite',
    r'\bnr\b': 'near',
    r'\bbhd\b': 'behind',
    r'\badj\b': 'adjacent',
    r'\bdist\b': 'district',
    r'\btaluk\b': 'taluk',
    r'\bbangalore\b': 'bengaluru',
    r'\bblr\b': 'bengaluru',
    r'\bb\'?lore\b': 'bengaluru',
    r'\bkarnataka\b': 'karnataka',
    r'\bk\.?a\.?\b': 'karnataka',
}

# ── Common Indian prefixes to standardize ──
NAME_PREFIXES = {
    r'^shree\s+': 'sri ',
    r'^shri\s+': 'sri ',
    r'^sree\s+': 'sri ',
    r'^m/s\.?\s*': '',
    r'^messrs\.?\s*': '',
}


def normalize_business_name(raw_name):
    """
    Normalize a business name for comparison.

    Steps:
    1. Lowercase
    2. Remove legal suffixes
    3. Standardize prefixes (Shree → Sri)
    4. Expand abbreviations
    5. Remove punctuation
    6. Collapse whitespace
    """
    if not raw_name:
        return ""

    name = raw_name.lower().strip()

    # Remove punctuation except alphanumeric and spaces
    name = re.sub(r'[^\w\s]', ' ', name)

    # Standardize prefixes
    for pattern, replacement in NAME_PREFIXES.items():
        name = re.sub(pattern, replacement, name)

    # Remove legal suffixes
    for pattern in LEGAL_SUFFIXES:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    # Remove Indian Stopwords
    for pattern in INDIAN_STOPWORDS:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    # Expand abbreviations
    for pattern, replacement in BIZ_ABBREVIATIONS.items():
        name = re.sub(pattern, replacement, name)

    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    return name


def normalize_address(raw_address):
    """
    Normalize an address for comparison.

    Steps:
    1. Lowercase
    2. Expand abbreviations
    3. Remove "near X" prefixes
    4. Remove punctuation
    5. Collapse whitespace
    """
    if not raw_address:
        return ""

    addr = raw_address.lower().strip()

    # Remove punctuation except alphanumeric, spaces, and hyphens
    addr = re.sub(r'[^\w\s\-]', ' ', addr)

    # Expand abbreviations
    for pattern, replacement in ADDR_ABBREVIATIONS.items():
        addr = re.sub(pattern, replacement, addr)

    # Remove common noise phrases
    addr = re.sub(r'\bnear\s+\w+(\s+\w+)?', '', addr)
    addr = re.sub(r'\bbehind\s+\w+(\s+\w+)?', '', addr)

    # Collapse whitespace
    addr = re.sub(r'\s+', ' ', addr).strip()

    return addr


def normalize_pan(pan):
    """Normalize PAN to uppercase, stripped."""
    if not pan:
        return None
    pan = pan.upper().strip()
    # Validate PAN format: ABCDE1234F
    if re.match(r'^[A-Z]{5}[0-9]{4}[A-Z]$', pan):
        return pan
    return None


def normalize_gstin(gstin):
    """Normalize GSTIN and extract PAN if valid."""
    if not gstin:
        return None, None
    gstin = gstin.upper().strip().replace(" ", "")
    # Validate GSTIN format: 29ABCDE1234F1Z5 (15 chars)
    if re.match(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]{3}$', gstin):
        pan = gstin[2:12]
        return gstin, pan
    return None, None


def extract_name_tokens(normalized_name):
    """Extract meaningful tokens from a normalized name, sorted."""
    if not normalized_name:
        return []
    # Remove very short tokens (likely noise)
    tokens = [t for t in normalized_name.split() if len(t) > 1]
    return sorted(tokens)


def extract_address_tokens(normalized_address):
    """Extract meaningful tokens from a normalized address, sorted."""
    if not normalized_address:
        return []
    tokens = [t for t in normalized_address.split() if len(t) > 1]
    return sorted(tokens)


def soundex(name):
    """
    Compute Soundex code for a name.
    Used for phonetic blocking.
    """
    if not name:
        return ""

    name = name.upper()
    name = re.sub(r'[^A-Z]', '', name)

    if not name:
        return ""

    # Soundex mapping
    mapping = {
        'B': '1', 'F': '1', 'P': '1', 'V': '1',
        'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2',
        'S': '2', 'X': '2', 'Z': '2',
        'D': '3', 'T': '3',
        'L': '4',
        'M': '5', 'N': '5',
        'R': '6',
    }

    first_letter = name[0]
    coded = first_letter

    prev_code = mapping.get(first_letter, '0')

    for char in name[1:]:
        code = mapping.get(char, '0')
        if code != '0' and code != prev_code:
            coded += code
        prev_code = code if code != '0' else prev_code

    # Pad or trim to 4 characters
    coded = (coded + '000')[:4]
    return coded


def compute_metaphone(name):
    """
    Compute Metaphone phonetic encoding.
    More accurate than soundex for Indian business name transliterations.
    """
    if not name:
        return ""
    code = doublemetaphone(name)
    return code[0] if code[0] else ""
