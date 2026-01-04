import requests
import zipfile
from pathlib import Path
from tqdm import tqdm
from src.lang_ai.core.utils import get_env_var, get_project_root
from src.lang_ai.core.logger import setup_logging

logger = setup_logging()


def download_surfdrive_folder(
    share_url: str, password: str, output_filename: str | Path, block_size: int = 8192
) -> None:
    """
    Downloads a file from a SurfDrive/Nextcloud public share using direct WebDAV access.

    This function authenticates using the shared token as the username (implied by the URL)
    or the specific WebDAV auth flow, streams the file content, and displays a progress bar.

    Args:
        share_url (str): The direct WebDAV URL to the file on SurfDrive.
        password (str): The password for the shared link.
        output_filename (str | Path): The local path where the downloaded file should be saved.
        block_size (int, optional): The chunk size for streaming the download in bytes. Defaults to 8192 (8KB).

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: If a network error occurs during the request.
    """

    output_path = Path(output_filename)
    logger.info(f"Target URL: {share_url}")
    logger.info(f"Downloading to '{output_path}'...")

    try:
        response = requests.get(share_url, auth=("password", password), stream=True)
        match response.status_code:
            case 200:
                # Ensure directory exists using pathlib
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Get total file size from headers for tqdm
                total_size_in_bytes = int(response.headers.get("content-length", 0))

                progress_bar = tqdm(
                    total=total_size_in_bytes, unit="iB", unit_scale=True
                )

                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=block_size):
                        progress_bar.update(len(chunk))
                        f.write(chunk)

                progress_bar.close()

                if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                    logger.error("ERROR, something went wrong")
                else:
                    logger.info(f"Success! Saved to '{output_path}'")
                    logger.info(f"Unzipping file to {output_path.parent}...")
                    try:
                        with zipfile.ZipFile(output_path, "r") as zip_ref:
                            zip_ref.extractall(output_path.parent)
                        logger.info("Unzip completed successfully.")
                    except zipfile.BadZipFile:
                        logger.error(
                            "Error: The downloaded file is not a valid zip file."
                        )
                    except Exception as e:
                        logger.error(f"Error during unzipping: {e}")
            case 401:
                logger.error("Error: Authentication failed. Please check the password.")
                return
            case _:
                logger.error(
                    f"Error: Failed to download. Status code: {response.status_code}"
                )
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


def download_pipeline() -> None:
    """
    Orchestrates the download and extraction of raw data from SurfDrive.

    Retrieves credentials from environment variables and sets the output path.
    """
    link, password = get_env_var("SURFDRIVE_LINK"), get_env_var("SURFDRIVE_PASSWORD")
    output_file = get_project_root() / "raw_data" / "raw_data.zip"
    download_surfdrive_folder(link, password, output_file)


if __name__ == "__main__":
    download_pipeline()
