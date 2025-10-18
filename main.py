# check_database.py
import psycopg2

def check_database_exists():
    print("🔍 Vérification de l'existence de la base de données...")
    
    try:
        # Connexion à PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="Postgres1234@",
            database="postgres"  # Connexion à la base par défaut
        )
        cursor = conn.cursor()
        
        # Vérifier si youtube_db existe
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'youtube_db'")
        exists = cursor.fetchone()
        
        if exists:
            print("✅ La base 'youtube_db' EXISTE dans PostgreSQL")
            
            # Tester la connexion à youtube_db
            try:
                conn_db = psycopg2.connect(
                    host="localhost",
                    port=5432, 
                    user="postgres",
                    password="Postgres1234@",
                    database="youtube_db"
                )
                print("✅ Connexion à 'youtube_db' RÉUSSIE")
                conn_db.close()
            except Exception as e:
                print(f"❌ Impossible de se connecter à 'youtube_db': {e}")
                
        else:
            print("❌ La base 'youtube_db' N'EXISTE PAS")
            
        cursor.close()
        conn.close()
        return exists
        
    except Exception as e:
        print(f"❌ Erreur de vérification: {e}")
        return False

if __name__ == "__main__":
    check_database_exists()