import pandas as pd
import uuid
import random
from datetime import datetime, timedelta
import os
import shutil

# -----------------------------------------------
# LOAD EXISTING 2016-2018 DATA
# -----------------------------------------------
print("📂 Loading existing data...")
orders   = pd.read_csv('data/olist_orders_dataset.csv')
customers = pd.read_csv('data/olist_customers_dataset.csv')
items    = pd.read_csv('data/olist_order_items_dataset.csv')
payments = pd.read_csv('data/olist_order_payments_dataset.csv')
reviews  = pd.read_csv('data/olist_order_reviews_dataset.csv')

os.makedirs('data/initial', exist_ok=True)
os.makedirs('data/new_batch', exist_ok=True)

# -----------------------------------------------
# STEP 0: Save original data as initial load
# -----------------------------------------------
print("\n💾 Saving original 2016-2018 data as initial load...")
for f in [
    'olist_orders_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_products_dataset.csv',
    'olist_sellers_dataset.csv',
    'olist_geolocation_dataset.csv',
    'product_category_name_translation.csv'
]:
    shutil.copy(f'data/{f}', f'data/initial/{f}')

print(f"   ✅ Initial load ready — {len(orders):,} orders (2016-2018)")

# -----------------------------------------------
# BATCH CONFIGURATION
# -----------------------------------------------
NUM_NEW_ORDERS   = len(orders) * 3       # 3x volume ~300k orders
NUM_RETURNING    = int(NUM_NEW_ORDERS * 0.70)  # 70% returning customers
NUM_NEW_CUST     = NUM_NEW_ORDERS - NUM_RETURNING  # 30% brand new customers

print(f"\n🔧 Generating 2019 batch:")
print(f"   Total new orders:    {NUM_NEW_ORDERS:,}")
print(f"   Returning customers: {NUM_RETURNING:,} (70%)")
print(f"   New customers:       {NUM_NEW_CUST:,} (30%)")

# -----------------------------------------------
# STEP 1: Generate new order_ids
# -----------------------------------------------
new_order_ids = [str(uuid.uuid4()) for _ in range(NUM_NEW_ORDERS)]

# -----------------------------------------------
# STEP 2: Mix returning (70%) + new (30%) customers
#
# In Olist's model, every purchase gets a NEW customer_id
# even for returning customers.
# customer_unique_id is what identifies the same real person.
#
# Returning customers:
#   - New customer_id (new purchase record)
#   - SAME customer_unique_id (same real person)
#
# New customers:
#   - New customer_id
#   - New customer_unique_id (never seen before)
# -----------------------------------------------
existing_unique_ids = customers['customer_unique_id'].tolist()

# 70% returning — reuse existing customer_unique_ids
returning_unique_ids  = random.choices(existing_unique_ids, k=NUM_RETURNING)
returning_customer_ids = [str(uuid.uuid4()) for _ in range(NUM_RETURNING)]

# 30% brand new — never seen before
new_unique_ids   = [str(uuid.uuid4()) for _ in range(NUM_NEW_CUST)]
new_customer_ids = [str(uuid.uuid4()) for _ in range(NUM_NEW_CUST)]

# Combine — returning first, then new
all_customer_ids = returning_customer_ids + new_customer_ids
all_unique_ids   = returning_unique_ids   + new_unique_ids

# -----------------------------------------------
# STEP 3: Generate 2019 timestamps (date progression)
# -----------------------------------------------
# Spread orders naturally across 2019
# Weighted toward mid-year (like real e-commerce patterns)
start_date = datetime(2019, 1, 1)
end_date   = datetime(2019, 12, 31)
date_range = (end_date - start_date).days

new_timestamps = sorted([  # sorted = natural date progression
    start_date + timedelta(
        days=random.randint(0, date_range),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    for _ in range(NUM_NEW_ORDERS)
])

print(f"\n📅 Date range of new batch:")
print(f"   First order:  {min(new_timestamps).strftime('%Y-%m-%d')}")
print(f"   Last order:   {max(new_timestamps).strftime('%Y-%m-%d')}")

# -----------------------------------------------
# STEP 4: Sample order statuses from real distribution
# -----------------------------------------------
status_dist = orders['order_status'].value_counts(normalize=True)
new_statuses = random.choices(
    status_dist.index.tolist(),
    weights=status_dist.values.tolist(),
    k=NUM_NEW_ORDERS
)

# -----------------------------------------------
# STEP 5: Build new orders table
# -----------------------------------------------
new_orders = pd.DataFrame({
    'order_id':                      new_order_ids,
    'customer_id':                   all_customer_ids,
    'order_status':                  new_statuses,
    'order_purchase_timestamp':      new_timestamps,
    'order_approved_at':             [t + timedelta(hours=1)  for t in new_timestamps],
    'order_delivered_carrier_date':  [t + timedelta(days=3)   for t in new_timestamps],
    'order_delivered_customer_date': [t + timedelta(days=7)   for t in new_timestamps],
    'order_estimated_delivery_date': [t + timedelta(days=10)  for t in new_timestamps],
})

# -----------------------------------------------
# STEP 6: Build new customers table
# -----------------------------------------------

# Returning customers — keep their city/state/zip (same person, same location)
returning_base = customers.sample(
    n=NUM_RETURNING, replace=True
).copy().reset_index(drop=True)
returning_base['customer_id']        = returning_customer_ids
returning_base['customer_unique_id'] = returning_unique_ids

# New customers — sample realistic Brazilian locations from existing data
new_cust_base = customers.sample(
    n=NUM_NEW_CUST, replace=True
).copy().reset_index(drop=True)
new_cust_base['customer_id']        = new_customer_ids
new_cust_base['customer_unique_id'] = new_unique_ids

# Combine
new_customers_df = pd.concat(
    [returning_base, new_cust_base],
    ignore_index=True
)

# -----------------------------------------------
# STEP 7: Build order_items, payments, reviews
# Reuse realistic values from existing data
# -----------------------------------------------
new_items = items.sample(
    n=NUM_NEW_ORDERS, replace=True
).copy().reset_index(drop=True)
new_items['order_id']      = new_order_ids
new_items['order_item_id'] = 1  # matches real avg of 1.14 items/order

new_payments = payments.sample(
    n=NUM_NEW_ORDERS, replace=True
).copy().reset_index(drop=True)
new_payments['order_id'] = new_order_ids

new_reviews = reviews.sample(
    n=NUM_NEW_ORDERS, replace=True
).copy().reset_index(drop=True)
new_reviews['order_id']   = new_order_ids
new_reviews['review_id']  = [str(uuid.uuid4()) for _ in range(NUM_NEW_ORDERS)]

# -----------------------------------------------
# STEP 8: Save new batch
# -----------------------------------------------
print("\n💾 Saving 2019 new batch...")

new_orders.to_csv(      'data/new_batch/olist_orders_dataset.csv',         index=False)
new_customers_df.to_csv('data/new_batch/olist_customers_dataset.csv',      index=False)
new_items.to_csv(       'data/new_batch/olist_order_items_dataset.csv',    index=False)
new_payments.to_csv(    'data/new_batch/olist_order_payments_dataset.csv', index=False)
new_reviews.to_csv(     'data/new_batch/olist_order_reviews_dataset.csv',  index=False)

# Reference tables — products, sellers, geolocation unchanged
for f in [
    'olist_products_dataset.csv',
    'olist_sellers_dataset.csv',
    'olist_geolocation_dataset.csv',
    'product_category_name_translation.csv'
]:
    shutil.copy(f'data/{f}', f'data/new_batch/{f}')

# -----------------------------------------------
# SUMMARY
# -----------------------------------------------
print(f"\n{'='*50}")
print(f"✅ BATCH GENERATION COMPLETE")
print(f"{'='*50}")
print(f"\n📦 data/initial/  (2016-2018 original data)")
print(f"   Orders:    {len(orders):,}")
print(f"   Customers: {len(customers):,}")

print(f"\n📦 data/new_batch/  (2019 simulated data)")
print(f"   Orders:    {len(new_orders):,}")
print(f"   Customers: {len(new_customers_df):,}")
print(f"             ↳ {NUM_RETURNING:,} returning (70%)")
print(f"             ↳ {NUM_NEW_CUST:,} brand new (30%)")
print(f"   Items:     {len(new_items):,}")
print(f"   Payments:  {len(new_payments):,}")
print(f"   Reviews:   {len(new_reviews):,}")

print(f"\n📅 2019 date range:")
print(f"   {min(new_timestamps).strftime('%B %d, %Y')} → "
      f"{max(new_timestamps).strftime('%B %d, %Y')}")

print(f"\n🚀 Next steps:")
print(f"   1. Run pipeline with IS_INCREMENTAL_RUN = False  (initial load)")
print(f"   2. Check BigQuery row counts")
print(f"   3. Set IS_INCREMENTAL_RUN = True  (new batch)")
print(f"   4. Run pipeline again")
print(f"   5. Verify row counts increased, zero duplicates")
print(f"   6. Compare BigQuery costs before vs after")
print(f"{'='*50}")