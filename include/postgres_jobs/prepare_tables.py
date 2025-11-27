from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def prepare_tables_callable():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    tables = {
        "products": """
            CREATE TABLE IF NOT EXISTS ecommerce_products (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                description TEXT,
                category VARCHAR(255),
                price DOUBLE PRECISION,
                discountPercentage DOUBLE PRECISION,
                rating DOUBLE PRECISION,
                stock INTEGER,
                tags TEXT,
                brand VARCHAR(255),
                sku VARCHAR(100),
                weight DOUBLE PRECISION,
                dimensions TEXT,
                warrantyInformation TEXT,
                shippingInformation TEXT,
                availabilityStatus VARCHAR(100),
                reviews TEXT,
                returnPolicy TEXT,
                minimumOrderQuantity INTEGER,
                meta TEXT,
                images TEXT
            );
        """,
        "users": """
            CREATE TABLE IF NOT EXISTS ecommerce_users (
                id SERIAL PRIMARY KEY,
                firstName VARCHAR(255),
                lastName VARCHAR(255),
                maidenName VARCHAR(255),
                age INTEGER,
                gender VARCHAR(50),
                email VARCHAR(255) UNIQUE,
                phone VARCHAR(50),
                username VARCHAR(255) UNIQUE,
                password VARCHAR(255),
                birthDate VARCHAR(50),
                image TEXT,
                bloodGroup VARCHAR(10),
                height DOUBLE PRECISION,
                weight DOUBLE PRECISION,
                ip TEXT,
                address TEXT,
                macAddress TEXT,
                university TEXT,
                bank TEXT,
                company TEXT,
                ein TEXT,
                ssn TEXT,
                userAgent TEXT,
                crypto TEXT,
                role VARCHAR(255)
            );
        """,
        "carts": """
            CREATE TABLE IF NOT EXISTS ecommerce_carts (
                id SERIAL PRIMARY KEY,
                products TEXT NOT NULL,
                total DOUBLE PRECISION NOT NULL,
                discountedTotal DOUBLE PRECISION,
                userId INTEGER NOT NULL,
                totalProducts INTEGER NOT NULL,
                totalQuantity INTEGER NOT NULL
            );
        """
    }

    for table_name, create_sql in tables.items():
        # Check if the table already exists
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='ecommerce_{table_name}');")
        exists = cursor.fetchone()[0]

        if exists:
            logging.info(f"✅ The table 'ecommerce_{table_name}' already exists.")
            # It's temporary.
            cursor.execute(f"TRUNCATE TABLE ecommerce_{table_name} RESTART IDENTITY CASCADE;")
            logging.info(f"The table 'ecommerce_{table_name}' was successfully emptied.")
        else:
            logging.info(f"⚠️ The table 'ecommerce_{table_name}' does not exist : creation in progress...")
            cursor.execute(create_sql)
            logging.info(f"✅ The table 'ecommerce_{table_name}' has been successfully created.")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("✅ Table checks and preparation complete.")