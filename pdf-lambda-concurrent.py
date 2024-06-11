import base64
import json
import os
import io
import urllib.parse
import boto3
import pdfplumber
import requests
from PIL import Image
from langchain_community.docstore.document import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
import concurrent.futures
from datetime import datetime

openai_key = os.getenv('OPENAI_API_KEY')
openai_org = os.getenv('OPENAI_ORGANIZATION')

s3_client = boto3.client('s3')
db_client = boto3.client('dynamodb')

embeddings = OpenAIEmbeddings(openai_api_key=openai_key, openai_organization=openai_org)

def store_db(job_id, filename, text):
    try:
        db_client.put_item(
            TableName='pdf-data-dump',
            Item={
                'file': {'S': f"{job_id}_{filename}"},
                'text': {'S': text}
            }
        )
        print(f"Successfully stored text for {filename} in DynamoDB.")
    except Exception as e:
        print(f"Error storing data in DynamoDB for {filename}: {e}")

def extract_text_from_image(png_data, page_text):
    base64_image = base64.b64encode(png_data).decode('utf-8')
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {openai_key}"
    }
    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f'''
                            *** {page_text} ***

                            Raw text in the page is given below Consider it as a option or clue for you.
                            The given image is related to the text given. 
                            Analyze the image and extract the text from it.
                            If the image has some visuals other than text then also explain what the visuals describe and consider the given text content before generating the description of the image if it is related.
                            Maintain the response in a small content don't give huge content.
                            Give only the content don't give headings or topics for that on your own.
                            Give the result in human readable format.
                        '''
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{base64_image}"
                        }
                    }
                ]
            }
        ],
        "max_tokens": 300
    }
    
    try:
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        response.raise_for_status()
        print(response.json()['choices'][0]['message']['content'])
        return response.json()['choices'][0]['message']['content'] + "\n"
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  
        print("Response:", response.text)  
    except Exception as err:
        print(f"Other error occurred: {err}") 
    return "" 

def process_pdf(file_path):
    try:
        text=''
        with pdfplumber.open(file_path) as pdf:
            for page_number, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                page_images_text = ""
                try:
                    for image in page.images:
                            img_data = image["stream"].get_rawdata()
                            page_images_text += extract_text_from_image(img_data,page_text) + "\n"
                except Exception as e:
                    print(f"Error extracting text from image : {e}")
                text += page_text + "\n" + page_images_text

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=975, chunk_overlap=100, length_function=len)
        split_texts = text_splitter.split_text(text)
        split_docs = [Document(page_content=t) for t in split_texts]
        
        return split_docs
    except Exception as e:
        print(f"Error processing PDF {file_path}: {e}")
        return []

def process_pdf_directory(directory_path, job_id):
    try:
        documents = []
        c = 0
        for filename in os.listdir(directory_path):
            page_text=''
            if filename.endswith(".pdf"):
                file_path = os.path.join(directory_path, filename)
                try:
                    docs = process_pdf(file_path)
                    if docs:
                        for doc in docs:
                            page_text+=doc.page_content+' \n '
                        documents.extend(docs)
                    store_db(job_id, filename, page_text)
                    c += 1
                except Exception as e:
                    print(f"Error processing file {filename}: {e}")
        return documents, c
    except Exception as e:
        print(f"Error processing PDF directory {directory_path}: {e}")
        return [], 0

def lambda_handler(event, context):
    try:
        st = datetime.now()
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        job_id = key.split("/")[1][len("repaired_"):]

        download_path = f"/tmp/{key[len('repaired-uploads/repaired_'):]}"
        os.makedirs(f"/tmp/{job_id}", exist_ok=True)
        
        s3_client.download_file(bucket, key, download_path)
        print(f"Downloaded {download_path}")
        
        docs, doc_count = process_pdf_directory(f"/tmp/{job_id}", job_id)
        input_count = len(os.listdir(f"/tmp/{job_id}"))
        doc_err = input_count - doc_count
        n_done = 0
        n_err = 0
        
        if docs:
            print("Accepted PDF")
            print("docs - ", len(docs))
            n_done = doc_count
        else:
            print("No documents found")
            n_err = 1

        print(f'Done: {n_done}\nError: {n_err}\n')

        et = datetime.now()
        time_difference = et - st
        print(time_difference.total_seconds())
        return {'statusCode': 200, 'body': json.dumps(f'Document processing completed.\nDocuments:\n\tTotal No. of Documents: {input_count}\n\tCompleted: {doc_count}\n\tError: {doc_err}\nWeaviate:\n\tProcessed: {n_done}\n\tErrors: {n_err}')}
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {'statusCode': 500, 'body': json.dumps(f'Error processing document: {e}')}

