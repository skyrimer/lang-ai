import requests
import os
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()


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
    print(f"Target URL: {share_url}")
    print(f"Downloading to '{output_path}'...")

    try:
        response = requests.get(share_url, auth=("password", password), stream=True)

        if response.status_code == 200:
            # Ensure directory exists using pathlib
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Get total file size from headers for tqdm
            total_size_in_bytes = int(response.headers.get("content-length", 0))

            progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=block_size):
                    progress_bar.update(len(chunk))
                    f.write(chunk)

            progress_bar.close()

            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                print("ERROR, something went wrong")
            else:
                print(f"Success! Saved to '{output_path}'")

        elif response.status_code == 401:
            print("Error: Authentication failed. Please check the password.")
        else:
            print(f"Error: Failed to download. Status code: {response.status_code}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Load secrets from environment variables
    LINK, PASSWORD = os.getenv("SURFDRIVE_LINK"), os.getenv("SURFDRIVE_PASSWORD")
    for required_var in [LINK, PASSWORD]:
        if not required_var:
            raise ValueError(
                "Environment variables SURFDRIVE_LINK and SURFDRIVE_PASSWORD are required.\
                Check that they are set in your .env file."
            )
    project_root = Path(__file__).resolve().parent.parent

    output_file = project_root / "raw_data" / "reddit_pol.csv"

    download_surfdrive_folder(LINK, PASSWORD, output_file)
