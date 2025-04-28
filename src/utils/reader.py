import zipfile
from pathlib import Path
from typing import List

class ZipCorpusReader:
    def __init__(self, zip_path):
        self.zip_path = Path(zip_path)
        if not self.zip_path.exists():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

    def list_documents(self) -> List[str]:
        with zipfile.ZipFile(self.zip_path, 'r') as zf:
            return [name for name in zf.namelist() if name.endswith('.txt')]

    @staticmethod
    def read_document(zf: zipfile.ZipFile, doc_name: str) -> str:
        with zf.open(doc_name) as file:
            content = file.read()
            return content.decode('utf-8', errors='ignore')
