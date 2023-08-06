from dotenv import load_dotenv

load_dotenv()

from scripts.etl_albums import EtlAlbums

if __name__ == "__main__":
    print("Corriendo script")
    etl = EtlAlbums()
    etl.run()
