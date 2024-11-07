import os
import requests
from collections import namedtuple
from unstructured.partition.auto import partition
from unstructured.documents.elements import Element
from langchain_core.documents.base import Document
from typing import List, Dict


SAVE_DIR = 'data/'
PdfUrl = namedtuple('PdfUrl', ['url', 'team'])
PDF_URLS = [
    PdfUrl("https://abc.xyz/assets/9a/bd/838c917c4b4ab21f94e84c3c2c65/goog-10-k-q4-2022.pdf", "google"),
    PdfUrl("https://abc.xyz/assets/43/44/675b83d7455885c4615d848d52a4/goog-10-k-2023.pdf", "google"),
    PdfUrl("https://d18rn0p25nwr6d.cloudfront.net/CIK-0000320193/b4266e40-1de6-4a34-9dfb-8632b8bd57e0.pdf", "apple"),
    PdfUrl("https://d18rn0p25nwr6d.cloudfront.net/CIK-0000320193/faab4555-c69b-438a-aaf7-e09305f87ca3.pdf", "apple"),
    PdfUrl("https://abc.xyz/assets/4a/c8/34b486974fedadb02099e3845df0/2021-alphabet-annual-report.pdf", "google")
]


os.makedirs(SAVE_DIR, exist_ok=True)


def download_file(url: str, save_path: str):
    """
    Downloads file from a given url
    """
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {save_path}")
    else:
        print(f"Failed to download: {url}")


def create_document_from_element(element: Element, team: str) -> Document:
    metadata = element.metadata.to_dict()
    metadata['element_id'] = element.id
    metadata['category'] = element.to_dict()['type']
    metadata['team'] = team
    document = Document(
        id=element.id,
        mimetype=metadata['filetype'],
        metadata=metadata,
        page_content=element.text
    )
    return document


def ingest_pdfs(pdf_urls: List[PdfUrl]) -> Dict[str, PdfUrl]:
    pdf_files = dict()
    for pdf_url in pdf_urls:
        file_name = pdf_url.url.split('/')[-1]
        save_path = os.path.join(SAVE_DIR, file_name)
        pdf_files[save_path] = pdf_url
        download_file(pdf_url.url, save_path)
    return pdf_files


def create_document_list(pdf_files: Dict[str, PdfUrl]) -> List[Document]:
    team_elements, pages = dict(), []
    for pdf_file_path, pdfurl in pdf_files.items():
        elements = partition(filename=pdf_file_path)
        team_elements[pdfurl.team] = elements
    for team, elements in team_elements.items():
        for element in elements:
            document = create_document_from_element(element, team)
            pages.append(document)
    return pages


def main():
    pdf_files = ingest_pdfs(PDF_URLS)
    return create_document_list(pdf_files)


if __name__ == "__main__":
    main()
