import json
import os
import logging
import psycopg2
from minio import Minio
from minio.commonconfig import CopySource
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("minio_to_postgres")

def get_conn():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        database=os.getenv("POSTGRES_DB", "energy_warehouse"),
        user=os.getenv("POSTGRES_USER", "energy_user"),
        password=os.getenv("POSTGRES_PASSWORD", "energy_pass")
    )

def get_minio():
    return Minio(
        os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000"),
        access_key=os.getenv("MINIO_USER", "admin"),
        secret_key=os.getenv("MINIO_PASSWORD", "password123"),
        secure=False
    )

def detect_schema(record: dict) -> str:
    return record.get("source", "unknown")

def insert_energy_price(cur, rec):
    cur.execute("""
        INSERT INTO energy_prices (period, area, product, process, series)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (period, series) DO NOTHING
    """, (rec["period"], rec["area"], rec["product"],
          rec["process"], rec["series"]))

def insert_commodity(cur, rec):
    cur.execute("""
        INSERT INTO commodity_spot_prices (commodity, date, price, unit)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (commodity, date) DO NOTHING
    """, (rec["commodity"], rec["date"], rec["price"], rec["unit"]))

INSERT_FN = {
    "energy_prices":        insert_energy_price,
    "commodity_spot_prices": insert_commodity,
}

def process_file(minio_client, conn, fname):
    response = minio_client.get_object("energy-raw", fname)
    data_bytes = response.read()
    response.close()
    response.release_conn()

    if not data_bytes.strip():
        log.warning("Fichier vide ignoré : %s", fname)
        return 0

    try:
        records = json.loads(data_bytes.decode("utf-8"))
    except json.JSONDecodeError as e:
        log.error("JSON invalide dans %s : %s", fname, e)
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for rec in records:
            schema = detect_schema(rec)
            if schema == "unknown":
                log.warning("Schéma inconnu pour record : %s", rec)
                continue
            try:
                INSERT_FN[schema](cur, rec)
                inserted += cur.rowcount
            except Exception as e:
                log.warning("Record ignoré : %s", e)
                conn.rollback()
        conn.commit()

    return inserted

def move_to_processed(minio_client, fname):
    try:
        minio_client.copy_object(
            "energy-processed", fname,
            CopySource("energy-raw", fname)
        )
        minio_client.remove_object("energy-raw", fname)
        log.info("Déplacé vers processed : %s", fname)
    except Exception as e:
        log.error("Erreur déplacement %s : %s", fname, e)

def run_minio_to_postgres():
    load_dotenv()
    minio_client = get_minio()
    conn = get_conn()

    files = [obj.object_name for obj in
             minio_client.list_objects("energy-raw")
             if obj.object_name.endswith(".json")]

    if not files:
        log.info("Aucun fichier dans energy-raw")
        conn.close()
        return {"files_processed": 0, "records_inserted": 0}

    log.info("%d fichier(s) à traiter", len(files))
    total_files, total_records = 0, 0

    for fname in files:
        log.info("Traitement : %s", fname)
        inserted = process_file(minio_client, conn, fname)
        move_to_processed(minio_client, fname)
        total_files += 1
        total_records += inserted
        log.info("  → %d enregistrements insérés", inserted)

    conn.close()
    log.info("ETL terminé — %d fichiers, %d enregistrements", total_files, total_records)
    return {"files_processed": total_files, "records_inserted": total_records}

if __name__ == "__main__":
    run_minio_to_postgres()