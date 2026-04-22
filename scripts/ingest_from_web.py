import os
import sys
import logging
import psycopg2
import pandas as pd
from pathlib import Path

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            options="-c search_path=heart360tk_schema"
        )
        # Apply the role immediately for permissions
        with conn.cursor() as cur:
            cur.execute("SET ROLE heart360tk;")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        sys.exit(1)

def clean_glucose_value(val):
    """Extracts numeric value from strings like '166 mg/dL'."""
    if pd.isna(val): return None
    try:
        # Splits '166 mg/dL' and takes the first part
        return float(str(val).split()[0])
    except (ValueError, IndexError):
        return None

def process_facility(conn, facility_name, files):
    logger.info(f"--- Processing Facility: {facility_name} ---")
    
    try:
        with conn.cursor() as cur:
            # 1. Upsert Org Unit Chain with updated Hierarchy
            # Levels: 0=Country, 1=Region, 2=District, 3=Facility
            hierarchy_names = ["Rwanda", "Test Region", "Test District", facility_name]
            hierarchy_levels = [0, 1, 2, 3]
            
            cur.execute("SELECT upsert_org_unit_chain(%s, %s)", (hierarchy_names, hierarchy_levels))
            org_unit_id = cur.fetchone()[0]
            logger.info(f"Hierarchy created/found: {' > '.join(hierarchy_names)} (ID: {org_unit_id})")

            # 2. Load Patients (from *_patients.csv)
            if 'patients' in files:
                df_p = pd.read_csv(files['patients'])
                for _, row in df_p.iterrows():
                    cur.execute("""
                        INSERT INTO patients (patient_id, patient_name, gender, patient_status, registration_date, org_unit_id)
                        VALUES (%s, %s, %s, 'ALIVE', %s, %s)
                        ON CONFLICT (patient_id) DO UPDATE SET
                            patient_name = EXCLUDED.patient_name,
                            gender = EXCLUDED.gender,
                            org_unit_id = EXCLUDED.org_unit_id;
                    """, (row['Patient ID'], row['Masked Name'], row['Sex'], row['Enrolled Date'], org_unit_id))
                logger.info(f"Patients: {len(df_p)} records processed.")

            # 3. Load Blood Pressure (from *_bp.csv)
            if 'bp' in files:
                df_bp = pd.read_csv(files['bp'])
                for _, row in df_bp.iterrows():
                    # Create/Find Encounter (Unique per Patient + Date)
                    cur.execute("""
                        INSERT INTO encounters (patient_id, encounter_date, org_unit_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (patient_id, encounter_date) DO UPDATE SET org_unit_id = EXCLUDED.org_unit_id
                        RETURNING id;
                    """, (row['Patient ID'], row['Measurement Date'], org_unit_id))
                    enc_id = cur.fetchone()[0]

                    # Load BP vitals linked to that encounter
                    cur.execute("""
                        INSERT INTO blood_pressures (encounter_id, systolic_bp, diastolic_bp)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (encounter_id) DO UPDATE SET
                            systolic_bp = EXCLUDED.systolic_bp,
                            diastolic_bp = EXCLUDED.diastolic_bp;
                    """, (enc_id, row['Systolic'], row['Diastolic']))
                logger.info(f"Blood Pressure: {len(df_bp)} records processed.")

            # 4. Load Blood Sugar (from *_glucose.csv)
            if 'glucose' in files:
                df_gl = pd.read_csv(files['glucose'])
                for _, row in df_gl.iterrows():
                    # Create/Find Encounter
                    cur.execute("""
                        INSERT INTO encounters (patient_id, encounter_date, org_unit_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (patient_id, encounter_date) DO UPDATE SET org_unit_id = EXCLUDED.org_unit_id
                        RETURNING id;
                    """, (row['Patient ID'], row['Measurement Date'], org_unit_id))
                    enc_id = cur.fetchone()[0]

                    # Clean the value (removes ' mg/dL') and load
                    sugar_val = clean_glucose_value(row['Value'])
                    cur.execute("""
                        INSERT INTO blood_sugars (encounter_id, blood_sugar_type, blood_sugar_value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (encounter_id) DO UPDATE SET
                            blood_sugar_value = EXCLUDED.blood_sugar_value;
                    """, (enc_id, row['Type'], sugar_val))
                logger.info(f"Blood Sugar: {len(df_gl)} records processed.")

            conn.commit()
            logger.info(f"Successfully finalized facility: {facility_name}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing {facility_name}: {e}")

def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python loader.py <folder_containing_csv_files>")
        sys.exit(1)

    data_dir = Path(sys.argv[1])
    if not data_dir.is_dir():
        logger.error(f"Directory not found: {data_dir}")
        sys.exit(1)

    # Grouping files by prefix (e.g., '003_Remera')
    # This ensures patient, bp, and glucose files for the same facility are processed together
    facility_groups = {}
    for f in data_dir.glob("*.csv"):
        parts = f.stem.split('_')
        if len(parts) < 3: continue
        
        prefix = "_".join(parts[:2]) # e.g., '003_Remera' or '004_Kazo'
        file_type = parts[-1].lower() # 'bp', 'glucose', or 'patients'

        if prefix not in facility_groups:
            facility_groups[prefix] = {}
        facility_groups[prefix][file_type] = f

    if not facility_groups:
        logger.warning(f"No valid CSV groups found in {data_dir}")
        return

    conn = get_connection()
    logger.info(f"Starting ingestion for {len(facility_groups)} facilities...")
    
    for facility, files in facility_groups.items():
        process_facility(conn, facility, files)
    
    conn.close()
    logger.info("Database ingestion cycle complete.")

if __name__ == "__main__":
    main()