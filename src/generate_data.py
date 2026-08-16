"""Generate deterministic synthetic source data for NullMarket Phase 6."""

from __future__ import annotations

import copy
import csv
import random
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import yaml

SEED = 42
NUM_STORES = 12
NUM_PRODUCTS = 100
NUM_ORDERS = 500
ORDER_START = datetime(2026, 1, 1, 8, 0, 0)
ORDER_DAYS = 90
INVENTORY_START = date(2026, 1, 1)
INVENTORY_SNAPSHOT_DAYS = (0, 14, 28, 42, 56, 70, 84)
SAMPLE_VALID_ROWS = 8

FIELDNAMES = {
    "orders": [
        "order_id",
        "customer_id",
        "store_id",
        "order_timestamp",
        "order_status",
        "payment_method",
        "channel",
    ],
    "order_items": [
        "order_id",
        "line_number",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "unit_cost",
        "list_price",
        "active_flag",
    ],
    "stores": [
        "store_id",
        "store_name",
        "city",
        "province",
        "region",
        "store_type",
        "open_date",
    ],
    "inventory_snapshots": [
        "snapshot_date",
        "store_id",
        "product_id",
        "quantity_on_hand",
        "reorder_level",
    ],
}

# These are synthetic value vocabularies, not additional business validation rules.
STORE_LOCATIONS = [
    ("Toronto", "Ontario", "Central"),
    ("Mississauga", "Ontario", "Central"),
    ("Ottawa", "Ontario", "Central"),
    ("Montreal", "Quebec", "East"),
    ("Quebec City", "Quebec", "East"),
    ("Vancouver", "British Columbia", "West"),
    ("Victoria", "British Columbia", "West"),
    ("Calgary", "Alberta", "West"),
    ("Edmonton", "Alberta", "West"),
    ("Winnipeg", "Manitoba", "Prairies"),
    ("Halifax", "Nova Scotia", "Atlantic"),
    ("Fredericton", "New Brunswick", "Atlantic"),
]
STORE_TYPES = ("Urban", "Suburban", "Compact")

PRODUCT_CATALOG = {
    "Grocery": ("Pantry", "Snacks", "Beverages"),
    "Electronics": ("Audio", "Accessories", "Small Devices"),
    "Home": ("Kitchen", "Cleaning", "Storage"),
    "Apparel": ("Basics", "Outerwear", "Accessories"),
    "Health & Beauty": ("Personal Care", "Skin Care", "Wellness"),
}
BRANDS = ("Northline", "MapleWorks", "TrueNorth", "Harbour", "Summit", "Cedar")
ORDER_STATUSES = ("Completed", "Pending", "Cancelled")
PAYMENT_METHODS = ("Credit Card", "Debit Card", "Digital Wallet")
CHANNELS = ("Store", "Online")


def money(value: Decimal | float | int | str) -> str:
    """Return a decimal value formatted to exactly two places."""
    return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def load_paths(project_root: Path) -> tuple[Path, Path]:
    config_path = project_root / "config" / "config.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    raw_path = project_root / config["paths"]["raw"]
    sample_path = project_root / "data" / "sample"
    return raw_path, sample_path


def generate_stores(rng: random.Random) -> list[dict]:
    rows = []
    for index, (city, province, region) in enumerate(STORE_LOCATIONS[:NUM_STORES], start=1):
        open_date = date(2010, 1, 1) + timedelta(days=rng.randint(0, 5000))
        rows.append(
            {
                "store_id": f"S{index:04d}",
                "store_name": f"NullMarket {city}",
                "city": city,
                "province": province,
                "region": region,
                "store_type": rng.choice(STORE_TYPES),
                "open_date": open_date.isoformat(),
            }
        )
    return rows


def generate_products(rng: random.Random) -> list[dict]:
    category_pairs = [
        (category, subcategory)
        for category, subcategories in PRODUCT_CATALOG.items()
        for subcategory in subcategories
    ]

    rows = []
    for index in range(1, NUM_PRODUCTS + 1):
        category, subcategory = rng.choice(category_pairs)
        brand = rng.choice(BRANDS)
        unit_cost = Decimal(rng.randint(250, 15000)) / Decimal(100)
        markup = Decimal(rng.randint(120, 220)) / Decimal(100)
        list_price = unit_cost * markup
        rows.append(
            {
                "product_id": f"P{index:05d}",
                "product_name": f"{brand} {subcategory} {index:03d}",
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "unit_cost": money(unit_cost),
                "list_price": money(list_price),
                "active_flag": rng.random() < 0.92,
            }
        )
    return rows


def generate_orders(rng: random.Random, stores: list[dict]) -> list[dict]:
    store_ids = [row["store_id"] for row in stores]
    rows = []

    for index in range(1, NUM_ORDERS + 1):
        day_offset = rng.randrange(ORDER_DAYS)
        second_offset = rng.randrange(12 * 60 * 60)
        order_timestamp = ORDER_START + timedelta(days=day_offset, seconds=second_offset)
        rows.append(
            {
                "order_id": f"O{index:06d}",
                "customer_id": f"C{rng.randint(1, 180):05d}",
                "store_id": rng.choice(store_ids),
                "order_timestamp": order_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "order_status": rng.choice(ORDER_STATUSES),
                "payment_method": rng.choice(PAYMENT_METHODS),
                "channel": rng.choice(CHANNELS),
            }
        )
    return rows


def generate_order_items(
    rng: random.Random, orders: list[dict], products: list[dict]
) -> list[dict]:
    product_by_id = {row["product_id"]: row for row in products}
    product_ids = list(product_by_id)
    rows = []

    for order in orders:
        line_count = rng.randint(1, 5)
        chosen_products = rng.sample(product_ids, k=line_count)

        for line_number, product_id in enumerate(chosen_products, start=1):
            product = product_by_id[product_id]
            quantity = rng.randint(1, 5)
            list_price = Decimal(product["list_price"])
            price_factor = Decimal(rng.randint(85, 100)) / Decimal(100)
            unit_price = (list_price * price_factor).quantize(Decimal("0.01"))
            gross_sales = unit_price * quantity
            discount_rate = Decimal(rng.choice((0, 0, 0, 5, 10, 15))) / Decimal(100)
            discount_amount = (gross_sales * discount_rate).quantize(Decimal("0.01"))

            rows.append(
                {
                    "order_id": order["order_id"],
                    "line_number": line_number,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": money(unit_price),
                    "discount_amount": money(discount_amount),
                }
            )
    return rows


def generate_inventory_snapshots(
    rng: random.Random, stores: list[dict], products: list[dict]
) -> list[dict]:
    rows = []
    for day_offset in INVENTORY_SNAPSHOT_DAYS:
        snapshot_date = INVENTORY_START + timedelta(days=day_offset)
        for store in stores:
            for product in products:
                reorder_level = rng.randint(5, 30)
                quantity_on_hand = rng.randint(0, 120)
                rows.append(
                    {
                        "snapshot_date": snapshot_date.isoformat(),
                        "store_id": store["store_id"],
                        "product_id": product["product_id"],
                        "quantity_on_hand": quantity_on_hand,
                        "reorder_level": reorder_level,
                    }
                )
    return rows


def generate_valid_datasets(seed: int = SEED) -> dict[str, list[dict]]:
    """Build all five datasets with valid keys and source relationships."""
    rng = random.Random(seed)
    stores = generate_stores(rng)
    products = generate_products(rng)
    orders = generate_orders(rng, stores)
    order_items = generate_order_items(rng, orders, products)
    inventory_snapshots = generate_inventory_snapshots(rng, stores, products)

    return {
        "orders": orders,
        "order_items": order_items,
        "products": products,
        "stores": stores,
        "inventory_snapshots": inventory_snapshots,
    }


def inject_known_defects(
    valid_datasets: dict[str, list[dict]],
) -> tuple[dict[str, list[dict]], dict[str, list[int]]]:
    """Inject deterministic defects only after valid datasets have been built."""
    datasets = copy.deepcopy(valid_datasets)
    defect_indexes: dict[str, list[int]] = {name: [] for name in datasets}

    # products: duplicate simple primary key
    datasets["products"].append(copy.deepcopy(datasets["products"][0]))
    defect_indexes["products"].append(len(datasets["products"]) - 1)

    # orders: missing required value and malformed timestamp
    datasets["orders"][1]["customer_id"] = None
    defect_indexes["orders"].append(1)
    datasets["orders"][2]["order_timestamp"] = "not-a-timestamp"
    defect_indexes["orders"].append(2)

    # order_items: duplicate composite key
    datasets["order_items"].append(copy.deepcopy(datasets["order_items"][0]))
    defect_indexes["order_items"].append(len(datasets["order_items"]) - 1)

    reference_order_id = datasets["orders"][3]["order_id"]
    valid_product_id = valid_datasets["products"][1]["product_id"]

    # order_items: invalid product foreign key
    datasets["order_items"].append(
        {
            "order_id": reference_order_id,
            "line_number": 9001,
            "product_id": "P99999",
            "quantity": 1,
            "unit_price": "10.00",
            "discount_amount": "0.00",
        }
    )
    defect_indexes["order_items"].append(len(datasets["order_items"]) - 1)

    # order_items: negative quantity
    datasets["order_items"].append(
        {
            "order_id": reference_order_id,
            "line_number": 9002,
            "product_id": valid_product_id,
            "quantity": -2,
            "unit_price": "10.00",
            "discount_amount": "0.00",
        }
    )
    defect_indexes["order_items"].append(len(datasets["order_items"]) - 1)

    # order_items: invalid negative price
    datasets["order_items"].append(
        {
            "order_id": reference_order_id,
            "line_number": 9003,
            "product_id": valid_product_id,
            "quantity": 1,
            "unit_price": "-1.00",
            "discount_amount": "0.00",
        }
    )
    defect_indexes["order_items"].append(len(datasets["order_items"]) - 1)

    # inventory_snapshots: invalid negative inventory value
    datasets["inventory_snapshots"][0]["quantity_on_hand"] = -5
    defect_indexes["inventory_snapshots"].append(0)

    return datasets, defect_indexes


def write_csv(path: Path, dataset_name: str, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES[dataset_name])
        writer.writeheader()
        writer.writerows(rows)


def sample_rows(rows: list[dict], defect_indexes: list[int]) -> list[dict]:
    selected_indexes = list(range(min(SAMPLE_VALID_ROWS, len(rows))))
    selected_indexes.extend(defect_indexes)
    # Preserve deterministic order while removing duplicate indexes.
    unique_indexes = list(dict.fromkeys(selected_indexes))
    return [rows[index] for index in unique_indexes]


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    raw_path, sample_path = load_paths(project_root)

    valid_datasets = generate_valid_datasets()
    datasets, defect_indexes = inject_known_defects(valid_datasets)

    for dataset_name, rows in datasets.items():
        write_csv(raw_path / f"{dataset_name}.csv", dataset_name, rows)
        write_csv(
            sample_path / f"{dataset_name}.csv",
            dataset_name,
            sample_rows(rows, defect_indexes[dataset_name]),
        )

    print(f"Synthetic data generated with fixed seed {SEED}.")
    print(f"Raw output: {raw_path}")
    print(f"Git samples: {sample_path}")
    for dataset_name in FIELDNAMES:
        print(
            f"  {dataset_name}: {len(datasets[dataset_name])} raw rows, "
            f"{len(sample_rows(datasets[dataset_name], defect_indexes[dataset_name]))} sample rows"
        )


if __name__ == "__main__":
    main()
