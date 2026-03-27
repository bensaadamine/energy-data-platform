import json
import os
import logging
import psycopg2
from minio import Minio
from minio.commonconfig import CopySource
from io import BytesIO
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("minio_to_postgres")

def run_minio_to_postgres():
    load_dotenv()

    MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")      # ← fix
    MINIO_USER       = os.getenv("MINIO_USER", "admin")
    MINIO_PASSWORD   = os.getenv("MINIO_PASSWORD", "password123")
    RAW_BUCKET       = "energy-raw"
    PROCESSED_BUCKET = "energy-processed"

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),               # ← fix
        database=os.getenv("POSTGRES_DB", "energy_warehouse"),
        user=os.getenv("POSTGRES_USER", "energy_user"),
        password=os.getenv("POSTGRES_PASSWORD", "energy_pass")
    )

    files = [obj.object_name for obj in minio_client.list_objects(RAW_BUCKET)
             if obj.object_name.endswith(".json")]

    if not files:
        log.info("Aucun fichier dans energy-raw — rien à traiter")
        conn.close()
        return {"files_processed": 0, "records_inserted": 0}

    log.info("%d fichier(s) trouvé(s) dans energy-raw", len(files))

    total_records = 0
    total_files = 0

    for fname in files:
        log.info("Traitement : %s", fname)
        response = minio_client.get_object(RAW_BUCKET, fname)
        data_bytes = response.read()
        response.close()
        response.release_conn()

        if not data_bytes.strip():
            log.warning("Fichier vide ignoré : %s", fname)
            _move_to_processed(minio_client, RAW_BUCKET, PROCESSED_BUCKET, fname)
            continue

        try:
            records = json.loads(data_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            log.error("JSON invalide dans %s : %s", fname, e)
            _move_to_processed(minio_client, RAW_BUCKET, PROCESSED_BUCKET, fname)
            continue

        inserted = _insert_records(conn, records)
        _move_to_processed(minio_client, RAW_BUCKET, PROCESSED_BUCKET, fname)

        total_files += 1
        total_records += inserted
        log.info("  → %d enregistrements insérés depuis %s", inserted, fname)

    conn.close()
    log.info("ETL terminé — %d fichiers, %d enregistrements insérés", total_files, total_records)
    return {"files_processed": total_files, "records_inserted": total_records}


def _insert_records(conn, records):
    inserted = 0
    with conn.cursor() as cur:
        for rec in records:
            try:
                cur.execute("""
                    INSERT INTO energy_prices (period, area, product, process, series)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (period, series) DO NOTHING
                """, (rec["period"], rec["area"], rec["product"],
                      rec["process"], rec["series"]))
                inserted += cur.rowcount
            except Exception as e:
                log.warning("Enregistrement ignoré %s : %s", rec, e)
                conn.rollback()
        conn.commit()
    return inserted


def _move_to_processed(minio_client, src_bucket, dst_bucket, fname):
    try:
        minio_client.copy_object(
            dst_bucket, fname,
            CopySource(src_bucket, fname)
        )
        minio_client.remove_object(src_bucket, fname)
        log.info("Déplacé vers energy-processed : %s", fname)
    except Exception as e:
        log.error("Erreur lors du déplacement de %s : %s", fname, e)


if __name__ == "__main__":
    run_minio_to_postgres()