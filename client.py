import subprocess
import pandas as pd
import json
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

config = json.load(open("config.json"))

logging.basicConfig(
    filename=config.get("log_filename"),
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

        path = os.path.join(config.get("local_store_dir"), file["id"])
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


def cleanup(file_or_dir):
    try:
        path = os.path.join(os.getcwd(), config.get("local_store_dir"), file_or_dir)
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)
            logging.info("Localfiles Cleanup complete...")

    except FileNotFoundError:
        logging.error(f"Error: {path} does not exist.")
    except OSError as e:
        logging.error(f"Error: {path} : {e.strerror}")


def split_input_file(split_by_col, variant="falstar"):
    def decorator(func):
        def wrapper(input_file):
            try:
                path = os.path.join(
                    os.getcwd(), config.get("local_store_dir"), input_file["id"]
                )
                input_filename = input_file["name"]
                input_filename_woext = os.path.splitext(input_filename)[0]
                setlog_path = config.get(variant).get("set_log")
                setreport_path = config.get(variant).get("set_report")
                dump = {}

                if not split_by_col:
                    if func(input_file):
                        dump.setdefault("setlogs", []).append(
                            os.path.join(path, f"{input_filename_woext}_{setlog_path}")
                        )
                        dump.setdefault("setreports", []).append(
                            os.path.join(
                                path, f"{input_filename_woext}_{setreport_path}"
                            )
                        )
                else:
                    logging.info("Splitting up the CSV file...")
                    df = pd.read_csv(os.path.join(path, input_file["name"]))
                    groups = df.groupby(split_by_col)

                    for name, group in groups:
                        fname = f"{input_filename_woext}_{name}.csv"
                        group.to_csv(os.path.join(path, fname), index=False)
                        dump.setdefault("fnames", []).append(fname)
                        if func({"name": fname, "id": input_file["id"]}):
                            dump.setdefault("setlogs", []).append(
                                os.path.join(
                                    path, f"{input_filename_woext}_{name}_{setlog_path}"
                                )
                            )
                            dump.setdefault("setreports", []).append(
                                os.path.join(
                                    path,
                                    f"{input_filename_woext}_{name}_{setreport_path}",
                                )
                            )

                if dump["setlogs"]:
                    pd.concat([pd.read_csv(fp) for fp in dump.get("setlogs")]).to_csv(
                        os.path.join(path, setlog_path), index=False
                    )

                if dump["setreports"]:
                    pd.concat(
                        [pd.read_csv(fp) for fp in dump.get("setreports")]
                    ).to_csv(os.path.join(path, setreport_path), index=False)

                for files in dump.values():
                    for file in files:
                        cleanup(os.path.join(input_file["id"], os.path.basename(file)))

                return True

            except FileNotFoundError as e:
                logging.error(f"FileNotFoundError: {str(e)}")

                return False

            except Exception as e:
                logging.error(f"An error occurred: {str(e)}")
                return False

        return wrapper

    return decorator


@split_input_file(config.get("split_by_col"))
def process(input_file):
    try:
        input_filename = input_file["name"]
        filename_woext = os.path.splitext(input_filename)[0]
        logging.info(f"Starting to process file {input_filename}")
        path = os.path.join(
            os.getcwd(), config.get("local_store_dir"), input_file["id"]
        )
        subprocess.call(
            [
                "scp",
                "-r",
                config.get("falstar").get("model_cfg_path"),
                path,
            ]
        )

        setlog_path = config.get("falstar").get("set_log")
        setreport_path = config.get("falstar").get("set_report")
        cfg_string = f'(include "models.cfg")\n(set-log "{filename_woext}_{setlog_path}")\
        \n(set-report "{filename_woext}_{setreport_path}")\n(validate "{input_filename}")'

        with open(
            os.path.join(path, f"{filename_woext}.cfg"),
            "w",
        ) as f:
            f.write(cfg_string)

        p = subprocess.check_output(
            [
                "bash",
                config.get("falstar").get("falstar_script_relpath"),
                str(f"{filename_woext}.cfg"),
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
        return False

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return False


def upload_files(service, base_folder_id, input_file):
    logging.info("Uploading files...")
    input_file_id = input_file["id"]
    input_filename_woext = os.path.splitext(input_file["name"])[0]

    path = os.path.join(os.getcwd(), config.get("local_store_dir"), input_file_id)
    output_csv_files = [
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
    for output_csv_filename in output_csv_files:
        try:
            # Move input file to results folder
            if output_csv_filename == input_file["name"]:
                f = (
                    service.files()
                    .update(
                        fileId=input_file_id,
                        addParents=output_folder_id,
                        removeParents=",".join(input_file.get("parents")),
                        fields="id, parents",
                    )
                    .execute()
                )
                logging.info(
                    f'"{output_csv_filename}" has been moved into output folder with ID: {f.get("id")}'
                )
                continue

            # Create a new file in Google Drive
            metadata = {
                "name": output_csv_filename,
                "parents": [output_folder_id],
                "mimeType": "text/csv",
            }
            media = MediaFileUpload(
                os.path.join(path, output_csv_filename), mimetype="text/csv"
            )
            f = (
                service.files()
                .create(body=metadata, media_body=media, fields="id")
                .execute()
            )
            logging.info(
                f'"{output_csv_filename}" has been uploaded to Google Drive with ID: {f.get("id")}'
            )

        except HttpError as error:
            logging.error(f"An error occurred: {error}")


def sync_log(service, folder_id):
    file_path = os.path.join(os.getcwd(), config.get("log_filename"))

    try:
        logging.info(f"Starting log file sync...")

        # Get the list of files in the Google Drive folder
        folder_query = "trashed=false and mimeType='application/vnd.google-apps.folder' and '{}' in parents".format(
            folder_id
        )
        folder_results = (
            service.files()
            .list(q=folder_query, fields="nextPageToken, files(id, name)")
            .execute()
        )
        folder_items = folder_results.get("files", [])

        # Find the file in the Google Drive folder
        file_query = "trashed=false and name='{}' and '{}' in parents".format(
            os.path.basename(file_path), folder_id
        )
        file_results = (
            service.files()
            .list(q=file_query, fields="nextPageToken, files(id, name)")
            .execute()
        )
        file_items = file_results.get("files", [])
        if len(file_items) > 0:
            file_id = file_items[0]["id"]
            file_metadata = {"name": os.path.basename(file_path)}
            media = MediaFileUpload(file_path, resumable=True)

            service.files().update(
                fileId=file_id, body=file_metadata, media_body=media, fields="id"
            ).execute()

        else:
            file_metadata = {
                "name": os.path.basename(file_path),
                "parents": [folder_id],
            }
            media = MediaFileUpload(file_path, resumable=True)
            service.files().create(
                body=file_metadata, media_body=media, fields="id"
            ).execute()
            logging.info(f"Creating new logfile '{file_metadata['name']}'")

        logging.info(f"Logfile sync complete.")
    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        logging.info(f"Logfile sync failed.")


def execute(service):
    try:
        base_folder_name = config.get("base_gdrive_folder_name")
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
        logging.info("Processing complete.")
        sync_log(service, base_folder["id"])
    except HttpError as error:
        if error.resp.status == 504:
            logging.error(
                "Timeout error: The request timed out. Please try again later."
            )
        else:
            logging.error(f"An error occurred: {error}")


# If modifying these scopes, delete the file token.json.
SCOPES = config.get("auth").get("scopes")


def main():
    logging.info("Starting...")
    creds = None
    if os.path.exists(config.get("auth").get("token")):
        creds = Credentials.from_authorized_user_file(
            config.get("auth").get("token"), SCOPES
        )

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logging.info("Requesting new token")
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                config.get("auth").get("creds"), SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(config.get("auth").get("token"), "w") as token:
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
