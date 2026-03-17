import os, time, glob
import pandas as pd

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", default="output/")
parser.add_argument("--mode", default="local")
parser.add_argument("--workers", type=int, default=4)
args = parser.parse_args()

os.makedirs(args.output, exist_ok=True)
t_start = time.time()

print("[INFO] Loading CSV files ...")
files = glob.glob(os.path.join(args.input, "*.csv"))
df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
print(f"[INFO] Loaded {len(df):,} rows")

purchases = df[df["event_type"]=="purchase"].copy()
purchases["spend"] = purchases["price"] * purchases["quantity"]

print("[INFO] Computing user features ...")
uf = df.groupby("user_id").agg(
    total_events=("event_id","count"),
    purchase_count=("event_type", lambda x:(x=="purchase").sum()),
    cart_add_count=("event_type", lambda x:(x=="add_to_cart").sum()),
    unique_products=("product_id","nunique"),
    unique_categories=("category","nunique"),
    session_count=("session_id","nunique"),
    avg_rating_given=("rating","mean")
).reset_index()
sp = purchases.groupby("user_id").agg(total_spend=("spend","sum"), avg_order_value=("spend","mean")).reset_index()
uf = uf.merge(sp, on="user_id", how="left").fillna(0)
uf["purchase_rate"] = uf["purchase_count"] / uf["total_events"]
uf.to_csv(os.path.join(args.output,"user_features.csv"), index=False)
print(f"[INFO] User features done: {len(uf):,} rows")

print("[INFO] Computing product features ...")
pf = df.groupby(["product_id","category"]).agg(
    total_interactions=("event_id","count"),
    view_count=("event_type", lambda x:(x=="view").sum()),
    purchase_count=("event_type", lambda x:(x=="purchase").sum()),
    cart_count=("event_type", lambda x:(x=="add_to_cart").sum()),
    avg_price=("price","mean"),
    unique_buyers=("user_id","nunique"),
    avg_rating=("rating","mean")
).reset_index()
rv = purchases.groupby("product_id").agg(total_revenue=("spend","sum")).reset_index()
pf = pf.merge(rv, on="product_id", how="left").fillna(0)
pf["conversion_rate"] = pf["purchase_count"] / pf["view_count"].replace(0,1)
pf.to_csv(os.path.join(args.output,"product_features.csv"), index=False)
print(f"[INFO] Product features done: {len(pf):,} rows")

print("[INFO] Computing category features ...")
cf = df.groupby("category").agg(
    total_events=("event_id","count"),
    purchase_count=("event_type", lambda x:(x=="purchase").sum()),
    avg_price=("price","mean"),
    unique_users=("user_id","nunique")
).reset_index()
cr = purchases.groupby("category").agg(total_revenue=("spend","sum")).reset_index()
cf = cf.merge(cr, on="category", how="left").fillna(0)
cf.to_csv(os.path.join(args.output,"category_features.csv"), index=False)
print(f"[INFO] Category features done: {len(cf):,} rows")

runtime = round(time.time() - t_start, 2)
mem_mb = round(df.memory_usage(deep=True).sum() / 1e6, 2)

print(f"\n==================================================")
print(f"  METRICS - {args.mode.upper()} MODE")
print(f"==================================================")
print(f"  Rows processed : {len(df):,}")
print(f"  Total runtime  : {runtime} seconds")
print(f"  Peak memory    : {mem_mb} MB")
print(f"==================================================")

with open(os.path.join(args.output, f"metrics_{args.mode}.txt"), "w") as f:
    f.write(f"Mode: {args.mode}\n")
    f.write(f"Workers: {args.workers}\n")
    f.write(f"Rows processed: {len(df)}\n")
    f.write(f"Total runtime seconds: {runtime}\n")
    f.write(f"Peak memory MB: {mem_mb}\n")
    f.write(f"Shuffle read MB: N/A\n")
    f.write(f"Shuffle write MB: N/A\n")

print("[INFO] Metrics saved!")
print("[INFO] Pipeline complete!")
