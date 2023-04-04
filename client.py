import subprocess
import pandas as pd
import io
import os
import shutil
import logging
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(
    filename="archcomp.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def download_csv_file(service, file):
    logger = logging.getLogger(__name__)
    try:
        request = service.files().get_media(fileId=file["id"])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(
                f"Downloaded {int(status.progress() * 100)}% of {file['name']}."
            )
        fh.seek(0)

        path = os.path.join("data", file["id"])
        if not os.path.exists(path):
            os.mkdir(path)

        file_path = os.path.join(path, file["name"])
        with open(file_path, "wb") as f:
            shutil.copyfileobj(fh, f)

    except HttpError as error:
        logger.error(f"An error occurred while downloading {file['name']}: {error}")
        raise

    return fh.getvalue()


def download_and_preprocess(service, input_file):
    name, ext = os.path.splitext(input_file["name"])
    if ext != ".csv":
        logging.info(f"{input_file['name']}: File skipped not a CSV file.")
        return False
    data = download_csv_file(service, input_file)
    try:
        validate(data)
    except Exception as e:
        logging.error(f"Validation failed: {str(e)}")
        cleanup(input_file["id"])
        return False
    return True


def validate(data):
    df = pd.read_csv(io.BytesIO(data))

    if "system" not in df.columns or "property" not in df.columns:
        raise ValueError("CSV data does not contain system or property headers")

    required_vals = set(
        ("AT", "CC", "AFC", "AFC_normal", "AFC_power", "NN", "SC", "F16")
    )
    system_vals = set(df["system"].tolist())

    if not system_vals.issubset(required_vals):
        raise ValueError("Invalid system values")


def cleanup(file_id):
    try:
        folder_path = os.path.join(os.getcwd(), "data", file_id)
        shutil.rmtree(folder_path)
        logging.info("Cleanup started..")

    except FileNotFoundError:
        logging.error(f"Error: {folder_path} does not exist.")
    except OSError as e:
        logging.error(f"Error: {folder_path} : {e.strerror}")


def process(input_file):
    try:
        input_filename = input_file["name"]
        logging.info(f"Startting to process file {input_filename}")
        path = os.path.join(os.getcwd(), "data", input_file["id"])
        subprocess.call(
            [
                "scp",
                "-r",
                "processing/validation/models.cfg",
                path,
            ]
        )

        cfg_string = f'(include "models.cfg")\n(set-log "validation-log.csv")\
        \n(set-report "validation-report.csv")\n(validate "{input_filename}")'

        filename_withoutext = input_filename.split(".")[0]
        with open(
            os.path.join(path, f"{filename_withoutext}.cfg"),
            "w",
        ) as f:
            f.write(cfg_string)

        p = subprocess.check_output(
            [
                "bash",
                "./../../processing/validation/falstar.sh",
                str(f"{filename_withoutext}.cfg"),
            ],
            cwd=str(path),
        )

        logging.info(p)

        if "Exception" in str(p):
            raise ValueError("Exception occured while running Falstar")

        logging.info("File processed")
        return True

    except ValueError as e:
        logging.error(f"Falstar error: {str(e)}")
        cleanup(input_file["id"])
        return False

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        cleanup(input_file["id"])
        return False


def upload_files(service, base_folder_id, input_file):
    logging.info("Uploading file")
    input_file_id = input_file["id"]
    input_filename_woext = input_file["name"].split(".")[0]

    path = os.path.join(os.getcwd(), "data", input_file_id)
    csv_files = [
        f
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f)) and f.endswith(".csv")
    ]

    query = "mimeType='application/vnd.google-apps.folder' and trashed=false and name='{}'".format(
        input_filename_woext
    )
    results = (
        service.files().list(q=query, fields="nextPageToken, files(id, name)").execute()
    )

    output_folder = None
    if len(results["files"]) == 0:
        output_folder_metadata = {
            "name": input_filename_woext,
            "parents": [base_folder_id],
            "mimeType": "application/vnd.google-apps.folder",
        }
        output_folder = (
            service.files().create(body=output_folder_metadata, fields="id").execute()
        )
    else:
        output_folder = results["files"][0]

    output_folder_id = output_folder.get("id")

    # Upload each CSV file to Google Drive
    try:
        for file_name in csv_files:
            # Move input file to results folder
            if file_name == input_file["name"]:
                file = (
                    service.files()
                    .update(
                        fileId=input_file_id,
                        addParents=[output_folder_id],
                        removeParents=input_file["parents"],
                        fields="id, parents",
                    )
                    .execute()
                )
                logging.info(
                    f'"{file_name}" has been moved in to output folder with ID: {file.get("id")}'
                )
                continue

            # Create a new file in Google Drive
            file_metadata = {
                "name": file_name,
                "parents": [output_folder_id],
                "mimeType": "text/csv",
            }
            media = MediaFileUpload(os.path.join(path, file_name), mimetype="text/csv")
            file = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            logging.info(
                f'"{file_name}" has been uploaded to Google Drive with ID: {file.get("id")}'
            )

    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        cleanup(input_file_id)
        raise


def execute(service):
    try:
        base_folder_name = "Archcomp"
        query = f"mimeType='application/vnd.google-apps.folder' and trashed=false and name='{base_folder_name}'"
        results = (
            service.files()
            .list(q=query, fields="nextPageToken, files(id, name)")
            .execute()
        )
        base_folder = None
        if len(results["files"]) == 0:
            folder_metadata = {
                "name": base_folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            base_folder = (
                service.files().create(body=folder_metadata, fields="id").execute()
            )
            logging.info(
                f"Base Folder '{base_folder_name}' created with ID: '{base_folder.get('id')}'"
            )
        else:
            base_folder = results["files"][0]
        query = f"'{base_folder['id']}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'"
        fields = "nextPageToken, files(id, name, parents)"
        results = service.files().list(q=query, fields=fields).execute()
        files = results.get("files", [])
        if not files:
            logging.info("No new files found.")
        else:
            for input_file in files:
                if not download_and_preprocess(service, input_file):
                    continue
                if process(input_file):
                    upload_files(service, base_folder.get("id"), input_file)
                cleanup(input_file["id"])

            while "nextPageToken" in results:
                page_token = results["nextPageToken"]
                results = (
                    service.files()
                    .list(q=query, fields=fields, pageToken=page_token)
                    .execute()
                )
                files = results.get("files", [])

                for input_file in files:
                    if not download_and_preprocess(service, input_file):
                        continue
                    if process(input_file):
                        upload_files(service, base_folder.get("id"), input_file)
                    cleanup(input_file["id"])
    except HttpError as error:
        if error.resp.status == 504:
            logging.error(
                "Timeout error: The request timed out. Please try again later."
            )
        else:
            logging.error(f"An error occurred: {error}")


# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]


def main():
    logging.info("Starting...")
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logging.info("Requesting new token")
        else:
            flow = InstalledAppFlow.from_client_secrets_file("creds.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)
        execute(service)
        logging.info("Exiting...")

    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        raise


if __name__ == "__main__":
    main()
