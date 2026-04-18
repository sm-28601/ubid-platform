"""
Synthetic Data Generator for UBID Platform.

Generates realistic business data across 4 Karnataka department systems
(Shop Establishment, Factories, Labour, KSPCB) for 2 Bengaluru Urban pin codes,
plus 12 months of activity events.

Key design:
- Same business appears with slightly different names/addresses across systems
- ~30% share PAN/GSTIN (easy anchoring)
- ~40% have partial overlaps (same address, similar name, no common ID)
- ~30% are genuinely different businesses
"""

import json
import random
import string
import sqlite3
import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.schema import get_connection, init_db

# ── Seed for reproducibility ──
random.seed(42)

# ── Configuration ──
PIN_CODES = ["560058", "560034"]
NUM_REAL_BUSINESSES = 100  # actual unique businesses
MONTHS_OF_EVENTS = 12

# ── Name pools ──
BUSINESS_PREFIXES = [
    "Sri", "Shree", "New", "Royal", "Golden", "Star", "Om", "Sai",
    "Lakshmi", "Ganesh", "Vishnu", "Mahalakshmi", "Annapurna", "Bharath",
    "National", "Modern", "Premier", "Classic", "Supreme", "Pioneer"
]

BUSINESS_CORES = [
    "Srinivasa", "Raghavendra", "Venkateshwara", "Krishna", "Rama",
    "Prakash", "Suresh", "Mahesh", "Deepak", "Arun", "Vijay",
    "Karnataka", "Bengaluru", "Mysore", "Mangalore", "Hubli",
    "Janata", "Bharathi", "Shakti", "Nandi", "Kaveri"
]

BUSINESS_SUFFIXES = [
    "Enterprises", "Industries", "Manufacturing", "Traders", "Associates",
    "Engineering", "Textiles", "Chemicals", "Foods", "Metals",
    "Polymers", "Electronics", "Auto Parts", "Garments", "Packaging",
    "Solutions", "Services", "Works", "Corporation", "Agency"
]

LEGAL_SUFFIXES = ["", "", "Pvt Ltd", "Private Limited", "LLP", "", ""]

OWNER_FIRST_NAMES = [
    "Ramesh", "Suresh", "Mahesh", "Ganesh", "Rajesh", "Naresh",
    "Venkatesh", "Lokesh", "Dinesh", "Yogesh", "Prakash", "Anil",
    "Sunil", "Kumar", "Ravi", "Srinivas", "Manjunath", "Basavaraj",
    "Shivakumar", "Chandrashekar", "Anand", "Murthy", "Gowda",
    "Reddy", "Naidu", "Shetty", "Hegde", "Patil", "Kulkarni", "Rao"
]

OWNER_LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Kumar", "Singh", "Reddy",
    "Naidu", "Gowda", "Shetty", "Hegde", "Patil", "Kulkarni",
    "Rao", "Murthy", "Prasad", "Iyengar", "Iyer", "Nair",
    "Pillai", "Acharya", "Bhat", "Joshi", "Kamath", "Pai"
]

STREET_NAMES = [
    "MG Road", "Brigade Road", "Commercial Street", "JC Road",
    "Bull Temple Road", "Hosur Road", "Bannerghatta Road",
    "Outer Ring Road", "Mysore Road", "Tumkur Road",
    "Kanakapura Road", "Sarjapur Road", "Whitefield Main Road",
    "HAL Airport Road", "Old Madras Road", "Bellary Road"
]

AREAS = [
    "Jayanagar", "Basavanagudi", "Rajajinagar", "Malleshwaram",
    "Indiranagar", "Koramangala", "BTM Layout", "HSR Layout",
    "Banashankari", "Vijayanagar", "Peenya", "Bommanahalli",
    "Yelahanka", "RT Nagar", "Hebbal", "Mahadevapura"
]

FACTORY_CATEGORIES = [
    "Textiles", "Chemicals", "Food Processing", "Metals & Alloys",
    "Plastics & Polymers", "Electronics", "Automobile Parts",
    "Pharmaceuticals", "Paper & Printing", "Cement & Construction",
    "Garments", "Leather", "Rubber", "Glass", "Machinery"
]

SHOP_CATEGORIES = [
    "Retail Trade", "Wholesale Trade", "Hotel & Restaurant",
    "Repair & Maintenance", "IT Services", "Financial Services",
    "Transport", "Storage & Warehousing", "Real Estate",
    "Professional Services", "Education", "Healthcare"
]

KSPCB_CONSENT_TYPES = ["Red", "Orange", "Green", "White"]

EVENT_TYPES = [
    "inspection_conducted", "license_renewed", "consent_renewed",
    "tax_filing_submitted", "compliance_notice_issued",
    "electricity_consumption", "water_consumption",
    "employee_report_filed", "pollution_test_passed",
    "pollution_test_failed", "closure_application"
]


def generate_pan():
    """Generate a realistic PAN (ABCDE1234F format)."""
    chars = ''.join(random.choices(string.ascii_uppercase, k=5))
    nums = ''.join(random.choices(string.digits, k=4))
    check = random.choice(string.ascii_uppercase)
    return f"{chars}{nums}{check}"


def generate_gstin(pan, state_code="29"):
    """Generate GSTIN from PAN. Format: 29ABCDE1234F1Z5"""
    entity_num = str(random.randint(1, 9))
    check_char = random.choice(string.ascii_uppercase + string.digits)
    return f"{state_code}{pan}{entity_num}Z{check_char}"


def create_name_variant(base_name, level="minor"):
    """Create realistic variants of a business name."""
    variants = []

    if level == "exact":
        return base_name

    # Common transformations
    transforms = [
        lambda n: n.upper(),
        lambda n: n.lower(),
        lambda n: n.replace("Enterprises", "Entrp"),
        lambda n: n.replace("Industries", "Ind"),
        lambda n: n.replace("Manufacturing", "Mfg"),
        lambda n: n.replace("Engineering", "Engg"),
        lambda n: n.replace("Private Limited", "Pvt Ltd"),
        lambda n: n.replace("Pvt Ltd", "Private Limited"),
        lambda n: n.replace("Sri", "Shree") if n.startswith("Sri") else n,
        lambda n: n.replace("Shree", "Sri") if n.startswith("Shree") else n,
        lambda n: "Sri " + n if not n.startswith("Sri") and random.random() > 0.5 else n,
        lambda n: n.replace("  ", " ").strip(),
        lambda n: n + " " + random.choice(["(Unit-1)", "(Regd)", ""]),
        lambda n: n.replace("a", "").replace("e", "") if random.random() > 0.7 else n,  # typos
    ]

    if level == "minor":
        # Apply 1-2 transforms
        result = base_name
        for _ in range(random.randint(1, 2)):
            t = random.choice(transforms)
            result = t(result)
        return result.strip()
    elif level == "major":
        # Apply 2-4 transforms
        result = base_name
        for _ in range(random.randint(2, 4)):
            t = random.choice(transforms)
            result = t(result)
        return result.strip()

    return base_name


def create_address_variant(base_address):
    """Create realistic variants of an address."""
    transforms = [
        lambda a: a.replace("Road", "Rd"),
        lambda a: a.replace("Rd", "Road"),
        lambda a: a.replace("Street", "St"),
        lambda a: a.replace("St", "Street"),
        lambda a: a.replace("No.", "Number"),
        lambda a: a.replace("Number", "No."),
        lambda a: a.replace("Cross", "Cr"),
        lambda a: a.replace("Main", "Mn"),
        lambda a: a.upper(),
        lambda a: a + ", Bengaluru" if "Bengaluru" not in a else a,
        lambda a: a.replace("Bengaluru", "Bangalore"),
        lambda a: a.replace(", ", ","),
        lambda a: "Near " + random.choice(["Bus Stop", "Temple", "Park", "School"]) + ", " + a,
    ]
    result = base_address
    for _ in range(random.randint(1, 3)):
        t = random.choice(transforms)
        result = t(result)
    return result.strip()


def create_owner_name_variant(base_name):
    """Create variants of owner name."""
    parts = base_name.split()
    transforms = [
        lambda: base_name,
        lambda: base_name.upper(),
        lambda: parts[0][0] + ". " + " ".join(parts[1:]) if len(parts) > 1 else base_name,
        lambda: " ".join(parts[1:]) + " " + parts[0] if len(parts) > 1 else base_name,
        lambda: parts[0] + " " + parts[-1][0] + "." if len(parts) > 1 else base_name,
    ]
    return random.choice(transforms)()


class BusinessEntity:
    """Represents a real-world business that may appear across multiple systems."""

    def __init__(self, idx):
        self.idx = idx
        self.core_name = f"{random.choice(BUSINESS_PREFIXES)} {random.choice(BUSINESS_CORES)} {random.choice(BUSINESS_SUFFIXES)}"
        self.legal_suffix = random.choice(LEGAL_SUFFIXES)
        self.full_name = f"{self.core_name} {self.legal_suffix}".strip()
        self.owner = f"{random.choice(OWNER_FIRST_NAMES)} {random.choice(OWNER_LAST_NAMES)}"
        self.pincode = random.choice(PIN_CODES)
        self.street_num = random.randint(1, 999)
        self.street = random.choice(STREET_NAMES)
        self.area = random.choice(AREAS)
        self.base_address = f"No. {self.street_num}, {self.street}, {self.area}, Bengaluru - {self.pincode}"
        self.factory_category = random.choice(FACTORY_CATEGORIES)
        self.shop_category = random.choice(SHOP_CATEGORIES)
        self.kspcb_consent = random.choice(KSPCB_CONSENT_TYPES)

        # PAN/GSTIN — not all businesses have both
        dice = random.random()
        if dice < 0.3:
            # Has both PAN and GSTIN
            self.pan = generate_pan()
            self.gstin = generate_gstin(self.pan)
        elif dice < 0.55:
            # Only PAN
            self.pan = generate_pan()
            self.gstin = None
        elif dice < 0.75:
            # Only GSTIN (PAN derivable from GSTIN)
            self.pan = generate_pan()
            self.gstin = generate_gstin(self.pan)
            # But some systems won't record the PAN separately
        else:
            # Neither
            self.pan = None
            self.gstin = None

        # Registration dates (spread over last 10 years)
        base_date = datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3650))
        self.reg_date = base_date.strftime("%Y-%m-%d")

        # Which systems this business appears in (at least 1, up to 4)
        all_systems = ["shop_establishment", "factories", "labour", "kspcb"]
        num_systems = random.choices([1, 2, 3, 4], weights=[15, 35, 35, 15])[0]
        self.systems = random.sample(all_systems, num_systems)

        # Activity level determines event generation
        self.activity_level = random.choices(
            ["high", "medium", "low", "none"],
            weights=[30, 35, 20, 15]
        )[0]


def generate_shop_establishment_record(biz, seq):
    """Generate a Shop Establishment record."""
    # Sometimes PAN/GSTIN missing in this system
    pan = biz.pan if random.random() > 0.3 else None
    gstin = biz.gstin if random.random() > 0.5 else None

    return {
        "source_system": "shop_establishment",
        "source_id": f"SE-BLR-{biz.pincode[-3:]}-{seq:04d}",
        "raw_name": create_name_variant(biz.full_name, "minor"),
        "raw_address": create_address_variant(biz.base_address),
        "pincode": biz.pincode,
        "pan": pan,
        "gstin": gstin,
        "owner_name": create_owner_name_variant(biz.owner),
        "registration_date": biz.reg_date,
        "category": biz.shop_category,
    }


def generate_factories_record(biz, seq):
    """Generate a Factories department record — tends to be more formal."""
    pan = biz.pan if random.random() > 0.15 else None
    gstin = biz.gstin if random.random() > 0.25 else None

    name = create_name_variant(biz.full_name, "minor")
    # Factories often use more formal names
    if "Pvt" not in name and random.random() > 0.5:
        name = name + " Pvt Ltd"

    return {
        "source_system": "factories",
        "source_id": f"FACT-KA-{seq:05d}-{random.randint(100, 999)}",
        "raw_name": name,
        "raw_address": create_address_variant(biz.base_address),
        "pincode": biz.pincode,
        "pan": pan,
        "gstin": gstin,
        "owner_name": biz.owner,  # factories tend to record full names
        "registration_date": biz.reg_date,
        "category": biz.factory_category,
    }


def generate_labour_record(biz, seq):
    """Generate a Labour department record — often abbreviated names, no GSTIN."""
    pan = biz.pan if random.random() > 0.4 else None

    return {
        "source_system": "labour",
        "source_id": f"LAB-{biz.pincode}-{seq:05d}",
        "raw_name": create_name_variant(biz.full_name, "major"),  # more variation
        "raw_address": create_address_variant(biz.base_address),
        "pincode": biz.pincode,
        "pan": pan,
        "gstin": None,  # Labour system doesn't capture GSTIN
        "owner_name": create_owner_name_variant(biz.owner),
        "registration_date": biz.reg_date,
        "category": biz.shop_category,
    }


def generate_kspcb_record(biz, seq):
    """Generate a KSPCB record — technical names."""
    gstin = biz.gstin if random.random() > 0.35 else None

    name = biz.full_name
    # KSPCB often adds "Unit" or "Plant"
    if random.random() > 0.5:
        name = name + f" - Unit {random.randint(1, 3)}"

    return {
        "source_system": "kspcb",
        "source_id": f"KSPCB-CFO-{seq:05d}-{random.randint(100, 999)}",
        "raw_name": create_name_variant(name, "minor"),
        "raw_address": create_address_variant(biz.base_address),
        "pincode": biz.pincode,
        "pan": None,  # KSPCB doesn't capture PAN
        "gstin": gstin,
        "owner_name": create_owner_name_variant(biz.owner),
        "registration_date": biz.reg_date,
        "category": biz.kspcb_consent,
    }


def generate_events(biz, ubid_placeholder):
    """Generate activity events for a business over the last 12 months."""
    events = []
    now = datetime(2025, 4, 1)

    if biz.activity_level == "none":
        # Last event was 20+ months ago
        last_event = now - timedelta(days=random.randint(600, 900))
        events.append({
            "source_system": random.choice(biz.systems),
            "event_type": "license_renewed",
            "event_date": last_event.strftime("%Y-%m-%d"),
            "event_details": json.dumps({"note": "Last known activity before going silent"}),
            "raw_identifier": biz.pan or biz.gstin or biz.full_name,
        })
        return events

    # Determine event frequency based on activity level
    if biz.activity_level == "high":
        num_events = random.randint(8, 20)
    elif biz.activity_level == "medium":
        num_events = random.randint(3, 8)
    else:  # low
        num_events = random.randint(1, 3)
        # Low activity — events are older
        now = now - timedelta(days=random.randint(180, 500))

    for _ in range(num_events):
        event_date = now - timedelta(days=random.randint(0, 365))
        event_type = random.choice([
            "inspection_conducted", "license_renewed", "consent_renewed",
            "tax_filing_submitted", "compliance_notice_issued",
            "electricity_consumption", "employee_report_filed",
            "pollution_test_passed"
        ])

        details = {"department": random.choice(biz.systems)}
        if event_type == "electricity_consumption":
            details["units_kwh"] = random.randint(100, 50000)
        elif event_type == "inspection_conducted":
            details["inspector_id"] = f"INS-{random.randint(1000, 9999)}"
            details["result"] = random.choice(["compliant", "non-compliant", "minor_observations"])
        elif event_type == "tax_filing_submitted":
            details["quarter"] = random.choice(["Q1", "Q2", "Q3", "Q4"])
            details["year"] = event_date.year

        # Use different identifiers to simulate real-world matching challenges
        if random.random() > 0.3 and biz.pan:
            raw_id = biz.pan
        elif random.random() > 0.4 and biz.gstin:
            raw_id = biz.gstin
        else:
            # Use source ID from one of the systems — harder to match
            raw_id = f"{random.choice(biz.systems)}:{biz.full_name}"

        events.append({
            "source_system": random.choice(biz.systems),
            "event_type": event_type,
            "event_date": event_date.strftime("%Y-%m-%d"),
            "event_details": json.dumps(details),
            "raw_identifier": raw_id,
        })

    return events


def generate_all_data():
    """Main generation function."""
    print("[GEN] Starting synthetic data generation...")

    # Initialize DB
    init_db()
    conn = get_connection()
    cursor = conn.cursor()

    # Clear existing data
    for table in ["source_records", "activity_events", "ubid_master",
                  "ubid_linkages", "match_candidates", "audit_log", "reviewer_feedback"]:
        cursor.execute(f"DELETE FROM {table}")

    businesses = []
    source_records = []
    all_events = []

    # Generate real businesses
    for i in range(NUM_REAL_BUSINESSES):
        biz = BusinessEntity(i)
        businesses.append(biz)

        gen_funcs = {
            "shop_establishment": generate_shop_establishment_record,
            "factories": generate_factories_record,
            "labour": generate_labour_record,
            "kspcb": generate_kspcb_record,
        }

        seq_counters = {"shop_establishment": 0, "factories": 0, "labour": 0, "kspcb": 0}
        for sys_name in biz.systems:
            seq_counters[sys_name] += 1
            record = gen_funcs[sys_name](biz, i * 10 + seq_counters[sys_name])
            record["_biz_idx"] = i  # internal tracking, not stored in DB
            source_records.append(record)

        # Generate events
        events = generate_events(biz, None)
        for evt in events:
            evt["_biz_idx"] = i
        all_events.extend(events)

    # Add some intra-department duplicates (same business, same department, different record)
    num_intra_dupes = 15
    for _ in range(num_intra_dupes):
        biz = random.choice(businesses)
        if biz.systems:
            sys_name = random.choice(biz.systems)
            gen_funcs = {
                "shop_establishment": generate_shop_establishment_record,
                "factories": generate_factories_record,
                "labour": generate_labour_record,
                "kspcb": generate_kspcb_record,
            }
            record = gen_funcs[sys_name](biz, random.randint(900, 999))
            record["source_id"] = record["source_id"] + f"-DUP-{random.randint(1, 999999)}"
            record["_biz_idx"] = biz.idx
            source_records.append(record)

    # Add some completely unique businesses (noise — different businesses)
    for i in range(30):
        noise_biz = BusinessEntity(NUM_REAL_BUSINESSES + i)
        noise_biz.systems = [random.choice(["shop_establishment", "factories", "labour", "kspcb"])]
        gen_funcs = {
            "shop_establishment": generate_shop_establishment_record,
            "factories": generate_factories_record,
            "labour": generate_labour_record,
            "kspcb": generate_kspcb_record,
        }
        sys_name = noise_biz.systems[0]
        record = gen_funcs[sys_name](noise_biz, 20000 + i)
        record["_biz_idx"] = NUM_REAL_BUSINESSES + i
        source_records.append(record)

    # Add some unmatched events (events with garbled identifiers)
    for _ in range(20):
        all_events.append({
            "source_system": random.choice(["shop_establishment", "factories", "labour", "kspcb"]),
            "event_type": random.choice(EVENT_TYPES),
            "event_date": (datetime(2025, 4, 1) - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            "event_details": json.dumps({"note": "Unmatched event with garbled identifier"}),
            "raw_identifier": f"UNKNOWN-{random.randint(10000, 99999)}",
            "_biz_idx": -1,
        })

    # Insert source records
    random.shuffle(source_records)
    for rec in source_records:
        biz_idx = rec.pop("_biz_idx", None)
        cursor.execute("""
            INSERT INTO source_records
            (source_system, source_id, raw_name, raw_address, pincode, pan, gstin,
             owner_name, registration_date, category, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rec["source_system"], rec["source_id"], rec["raw_name"],
            rec["raw_address"], rec["pincode"], rec.get("pan"),
            rec.get("gstin"), rec.get("owner_name"),
            rec.get("registration_date"), rec.get("category"),
            json.dumps(rec)
        ))

    # Insert events
    for evt in all_events:
        evt.pop("_biz_idx", None)
        cursor.execute("""
            INSERT INTO activity_events
            (source_system, event_type, event_date, event_details, raw_identifier)
            VALUES (?, ?, ?, ?, ?)
        """, (
            evt["source_system"], evt["event_type"], evt["event_date"],
            evt["event_details"], evt["raw_identifier"]
        ))

    conn.commit()

    # Print summary
    cursor.execute("SELECT COUNT(*) FROM source_records")
    num_records = cursor.fetchone()[0]
    cursor.execute("SELECT source_system, COUNT(*) FROM source_records GROUP BY source_system")
    by_system = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute("SELECT COUNT(*) FROM activity_events")
    num_events = cursor.fetchone()[0]

    print(f"\n[GEN] === Synthetic Data Summary ===")
    print(f"[GEN] Real businesses: {NUM_REAL_BUSINESSES}")
    print(f"[GEN] Total source records: {num_records}")
    for sys_name, count in sorted(by_system.items()):
        print(f"[GEN]   {sys_name}: {count}")
    print(f"[GEN] Total activity events: {num_events}")
    print(f"[GEN] Intra-department duplicates: {num_intra_dupes}")
    print(f"[GEN] Noise businesses: 30")
    print(f"[GEN] Unmatched events: 20")
    print(f"[GEN] ================================\n")

    conn.close()
    return num_records, num_events


if __name__ == "__main__":
    generate_all_data()
