import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Thông tin kết nối đến PostgreSQL
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password'
}

# Đường dẫn đến các file CSV
file_paths = {
    'dimCoin': 'path_to_dimCoin.csv',
    'dimCompany': 'path_to_dimCompany.csv',
    'dimTime': 'path_to_dimTime.csv',
    'factStocks': 'path_to_factStocks.csv',
    'factCoins': 'path_to_factCoins.csv'
}

# Tên các bảng tạm trong PostgreSQL
table_names = {
    'dimCoin': 'staging_dim_coin',
    'dimCompany': 'staging_dim_company',
    'dimTime': 'staging_dim_time',
    'factStocks': 'staging_fact_stocks',
    'factCoins': 'staging_fact_coins'
}

# Tạo cơ sở dữ liệu nếu chưa tồn tại
def create_database():
    conn_params = {
        'host': db_params['host'],
        'port': db_params['port'],
        'user': db_params['user'],
        'password': db_params['password']
    }
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Tạo cơ sở dữ liệu nếu chưa tồn tại
        cursor.execute(f"CREATE DATABASE {db_params['database']}")
        print(f"Database '{db_params['database']}' created successfully.")
        
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")

# Tạo kết nối đến cơ sở dữ liệu PostgreSQL
def create_connection():
    return psycopg2.connect(
        host=db_params['host'],
        port=db_params['port'],
        database=db_params['database'],
        user=db_params['user'],
        password=db_params['password']
    )

# Tạo bảng và tải dữ liệu từ CSV vào PostgreSQL
def etl_process():
    try:
        # Tạo kết nối đến cơ sở dữ liệu
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

        # Đọc từng file CSV và tải vào PostgreSQL
        for key, file_path in file_paths.items():
            # Đọc dữ liệu từ file CSV
            df = pd.read_csv(file_path)
            
            # Ghi dữ liệu vào PostgreSQL
            df.to_sql(table_names[key], engine, if_exists='replace', index=False)

        print("ETL process completed.")
    except Exception as e:
        print(f"Error during ETL process: {e}")

# Thực thi lần lượt các bước
if __name__ == "__main__":
    create_database()  # Tạo cơ sở dữ liệu nếu chưa tồn tại
    etl_process()      # Thực hiện ETL từ file CSV vào PostgreSQL
