import azure.functions as func
import requests, logging, os
# from get_secret import get_secret   # <-- remove this
from blob_connection import get_blob_url, saveblob
from azure.storage.blob import BlobClient
from docx import Document
import io
from pathlib import Path
from docx.shared import Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from datetime import datetime
import time
import json

bp = func.Blueprint() 

@bp.function_name("TranscriptietoolWebApp")
@bp.blob_trigger(
    arg_name="inblob",
    path="hhh-inputfolder/{name}",
    connection="AzureWebJobsStorage"
)
def TranscriptietoolWebApp(inblob: func.InputStream):
    try:
        ## init variable and basic headers
        container_name = 'hhh-inputfolder'

        # Read from Function App setting "APIKEYAssemblyAI"
        api_key = os.environ["APIKEYAssemblyAI"]  # raises KeyError if missing (good for visibility)
        # alternatively: api_key = os.getenv("APIKEYAssemblyAI", "")
        headers = {"authorization": api_key, "content-type": "application/json"}

        fileName = inblob.name.split("/", 1)
        logging.info("init variable done")
        
        ## Upload audio file to assembly
        sas_url = get_blob_url(container_name=container_name, blob_name=fileName[1])
        
        # Create a client and download the blob into memory
        blob_client = BlobClient.from_blob_url(sas_url)
        props = blob_client.get_blob_properties()
        metadata = props.metadata  # dict[str, str]
        
        instructions = metadata.get('instructions') or ''
        blob_type = metadata.get('type') or 'transcript'
        tenantId = metadata.get('tenantId') or ''
        
        prompt = f"maak een {blob_type} {instructions}"
        
        stream = blob_client.download_blob()
        audio_bytes = stream.readall()
        
        # make request
        r = requests.post(
            "https://api.eu.assemblyai.com/v2/upload",
            headers=headers, data=audio_bytes, timeout=30
        )
        r.raise_for_status()
            
        data = r.json()
        try:
            upload_url = data["upload_url"]
        except Exception:
            logging.error("No upload URL") 
            return None
        
        logging.info("Uploaded audio file")      
        
        ## Transcription start 
        payload = {
            "audio_url": upload_url,
            "speaker_labels": "true",
            "speakers_expected": 3,
            "language_detection": "true"
        }

        r = requests.post(
            "https://api.eu.assemblyai.com/v2/transcript",
            headers=headers, json=payload, timeout=30
        )
        r.raise_for_status()
        data = r.json()
        transcriptid = data['id']
        
        logging.info("Started Transcription")
        
        ## Pulse Transcription
        response = wait_for_transcript(request_id=transcriptid, headers=headers)  
        logging.info("Finished Transcription")
                
        if (blob_type != "transcript"):
            ## Get LeMUR  
            payload = {
                "final_model": "anthropic/claude-3-5-sonnet",
                "prompt": prompt,
                "max_output_size": 4000,
                "temperature": 0,
                "transcript_ids": [transcriptid]
            }
            r = requests.post(
                "https://api.eu.assemblyai.com/lemur/v3/generate/task",
                headers=headers, json=payload, timeout=30
            )
            r.raise_for_status()
            data = r.json() 
            request_id = data['request_id']
            response = data['response']
            logging.info("Finished LeMUR")  

            ## Delete LeMUR
            r = requests.delete(
                f"https://api.eu.assemblyai.com/lemur/v3/{request_id}",
                headers=headers, timeout=30
            )
            if r.status_code == 200:
                logging.info(f"LeMUR is correctly deleted, {request_id}")
            else:
                logging.error(f"LeMUR not deleted, ID:{request_id}")
                
        ## Delete Transcription
        r = requests.delete(
            f"https://api.eu.assemblyai.com/v2/transcript/{transcriptid}",
            headers=headers, timeout=30
        )
        if r.status_code == 200:
            logging.info(f"Transcript is correctly deleted, {transcriptid}")
        else:
            logging.error(f"Transcript not deleted, ID:{transcriptid}")
                               
        buf = generate_word_document(response)
        
        p = Path(inblob.name)
        file_stem = p.stem
        folder_id = p.parent.name
        
        saveblob(
            file_name=f"{folder_id}/{file_stem}.docx",
            data=buf.getvalue(),
            container_name="hhh-outputfolder"
        )
        logging.info("File uploaded and function done!")
        
    except requests.HTTPError as e:
        logging.error(f"Request failed: {e}")
    except Exception as ex:
        logging.error(ex)


def wait_for_transcript(
    request_id: str,
    headers: dict,
    poll_interval: float = 3.0,
    timeout: float = 3600.0
) -> dict:
    start = time.monotonic()
    url = f"https://api.eu.assemblyai.com/v2/transcript/{request_id}"
    while True:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", "").lower()
            if status == "completed":
                return data
            if status == "error":
                raise RuntimeError(f"Transcription failed: {data}")
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            raise TimeoutError(f"Polling timed out after {timeout:.1f}s")
        logging.info("Polling")
        time.sleep(poll_interval)


def generate_word_document(data: dict):
    # ... unchanged ...
    import io
    from docx import Document
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from datetime import datetime

    logging.info("Start met genereren van Word-document met aangepaste JSON-structuur")
    # (rest of your function exactly as before)
    # ...
    doc_io = io.BytesIO()
    # save and return
    return doc_io
