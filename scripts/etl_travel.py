import pandas as pd 
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUTS_DIR = BASE_DIR / "outputs"


def log(message: str) -> None:
    print(f"[INFO] {message}")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    log("Chargement des fichiers CSV...")

    trips = pd.read_csv(RAW_DIR / "trips.csv")
    bookings = pd.read_csv(RAW_DIR / "bookings.csv")
    suppliers = pd.read_csv(RAW_DIR / "suppliers.csv")
    employees = pd.read_csv(RAW_DIR / "employees.csv")
    budgets = pd.read_csv(RAW_DIR / "budgets.csv")

    return trips, bookings, suppliers, employees, budgets


def clean_data(
    trips: pd.DataFrame,
    bookings: pd.DataFrame,
    suppliers: pd.DataFrame,
    employees: pd.DataFrame,
    budgets: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    log("Nettoyage des données...")

    date_columns_trips = ["departure_date", "return_date"]
    for col in date_columns_trips:
        trips[col] = pd.to_datetime(trips[col], errors="coerce")

    bookings["booking_date"] = pd.to_datetime(bookings["booking_date"], errors="coerce")
    bookings["price"] = pd.to_numeric(bookings["price"], errors="coerce")

    trips.columns = trips.columns.str.strip().str.lower()
    bookings.columns = bookings.columns.str.strip().str.lower()
    suppliers.columns = suppliers.columns.str.strip().str.lower()
    employees.columns = employees.columns.str.strip().str.lower()
    budgets.columns = budgets.columns.str.strip().str.lower()

    bookings["status"] = bookings["status"].str.lower().str.strip()
    bookings["booking_type"] = bookings["booking_type"].str.lower().str.strip()
    suppliers["supplier_type"] = suppliers["supplier_type"].str.lower().str.strip()

    return trips, bookings, suppliers, employees, budgets


def transform_data(
    trips: pd.DataFrame,
    bookings: pd.DataFrame,
    suppliers: pd.DataFrame,
    employees: pd.DataFrame,
    budgets: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    log("Transformation et enrichissement des données...")

    merged = bookings.merge(trips, on="trip_id", how="left")
    merged = merged.merge(suppliers, on="supplier_id", how="left")
    merged = merged.merge(
        employees[["employee_id", "employee_name", "role"]],
        on="employee_id",
        how="left"
    )

    merged["trip_duration_days"] = (
        merged["return_date"] - merged["departure_date"]
    ).dt.days

    merged["booking_lead_days"] = (
        merged["departure_date"] - merged["booking_date"]
    ).dt.days

    merged["is_cancelled"] = merged["status"].eq("cancelled").astype(int)
    merged["validated_spend"] = merged["price"].where(merged["status"] == "confirmed", 0)

    trip_summary = (
        merged.groupby(["trip_id", "team"], as_index=False)
        .agg(
            total_trip_spend=("validated_spend", "sum"),
            total_bookings=("booking_id", "count"),
            cancelled_bookings=("is_cancelled", "sum"),
            avg_booking_lead_days=("booking_lead_days", "mean"),
            trip_duration_days=("trip_duration_days", "max"),
        )
    )

    trip_summary["cancellation_rate"] = (
        trip_summary["cancelled_bookings"] / trip_summary["total_bookings"]
    )

    budget_summary = (
        trip_summary.groupby("team", as_index=False)
        .agg(
            total_spend=("total_trip_spend", "sum"),
            total_trips=("trip_id", "count"),
            avg_cancellation_rate=("cancellation_rate", "mean"),
        )
        .merge(budgets, on="team", how="left")
    )

    budget_summary["budget_gap"] = budget_summary["monthly_budget"] - budget_summary["total_spend"]

    return merged, budget_summary


def export_data(merged: pd.DataFrame, budget_summary: pd.DataFrame) -> None:
    log("Export des fichiers transformés...")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    merged.to_csv(PROCESSED_DIR / "travel_bookings_enriched.csv", index=False)
    budget_summary.to_csv(OUTPUTS_DIR / "travel_budget_summary.csv", index=False)

    log("Fichiers exportés avec succès.")

def load_to_postgres(merged: pd.DataFrame, budget_summary: pd.DataFrame) -> None:
    log("Chargement dans PostgreSQL...")

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="travel_db",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS travel_bookings;
        CREATE TABLE travel_bookings (
            booking_id INTEGER,
            trip_id INTEGER,
            supplier_id INTEGER,
            booking_type TEXT,
            booking_date TIMESTAMP,
            price NUMERIC,
            status TEXT,
            team TEXT,
            employee_id INTEGER,
            departure_city TEXT,
            arrival_city TEXT,
            departure_date TIMESTAMP,
            return_date TIMESTAMP,
            supplier_name TEXT,
            supplier_type TEXT,
            employee_name TEXT,
            role TEXT,
            trip_duration_days INTEGER,
            booking_lead_days INTEGER,
            is_cancelled INTEGER,
            validated_spend NUMERIC
        );
    """)

    booking_rows = []
    for row in merged.itertuples(index=False):
        booking_rows.append(tuple(row))

    execute_values(
        cur,
        """
        INSERT INTO travel_bookings (
            booking_id, trip_id, supplier_id, booking_type, booking_date, price, status,
            team, employee_id, departure_city, arrival_city, departure_date, return_date,
            supplier_name, supplier_type, employee_name, role, trip_duration_days,
            booking_lead_days, is_cancelled, validated_spend
        ) VALUES %s
        """,
        booking_rows
    )

    cur.execute("""
        DROP TABLE IF EXISTS travel_budget;
        CREATE TABLE travel_budget (
            team TEXT,
            total_spend NUMERIC,
            total_trips INTEGER,
            avg_cancellation_rate NUMERIC,
            monthly_budget NUMERIC,
            budget_gap NUMERIC
        );
    """)

    budget_rows = []
    for row in budget_summary.itertuples(index=False):
        budget_rows.append(tuple(row))

    execute_values(
        cur,
        """
        INSERT INTO travel_budget (
            team, total_spend, total_trips, avg_cancellation_rate, monthly_budget, budget_gap
        ) VALUES %s
        """,
        budget_rows
    )

    conn.commit()
    cur.close()
    conn.close()

    log("Données chargées dans PostgreSQL.")

def main() -> None:
    log("Début du pipeline ETL Travel")
    trips, bookings, suppliers, employees, budgets = load_data()
    trips, bookings, suppliers, employees, budgets = clean_data(
        trips, bookings, suppliers, employees, budgets
    )
    merged, budget_summary = transform_data(
        trips, bookings, suppliers, employees, budgets
    )
    export_data(merged, budget_summary)
    load_to_postgres(merged, budget_summary)
    log("Pipeline terminé avec succès.")




if __name__ == "__main__":
    main()


