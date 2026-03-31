"""
Cleanup — delete old report.xlsx files from partner folders
============================================================
Run this once to remove the report.xlsx files created during the first run.
The current reports are named report_presmluvneni.xlsx.

USAGE:
    python cleanup_old_reports.py
"""

import os
from urllib.parse import urlparse
from dotenv import load_dotenv
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

load_dotenv("config.env")

SHAREPOINT_URL   = os.getenv("SHAREPOINT_URL", "https://cissrocz.sharepoint.com/sites/PE-Obchod")
SP_CLIENT_ID     = os.getenv("SP_CLIENT_ID", "")
SP_CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET", "")
BASE_FOLDER      = os.getenv("BASE_FOLDER", "Sdilene dokumenty/partner_reports")

# Connect
ctx = ClientContext(SHAREPOINT_URL).with_credentials(
    ClientCredential(SP_CLIENT_ID, SP_CLIENT_SECRET)
)
ctx.web.get().execute_query()
print("Connected to SharePoint.")

# Get all subfolders in partner_reports
site_path = urlparse(SHAREPOINT_URL).path.rstrip("/")
base_srv  = f"{site_path}/{BASE_FOLDER}"

folders = ctx.web.get_folder_by_server_relative_url(base_srv) \
              .folders.get().execute_query()

deleted = 0
for folder in folders:
    file_url = f"{folder.serverRelativeUrl}/report.xlsx"
    try:
        ctx.web.get_file_by_server_relative_url(file_url) \
            .delete_object().execute_query()
        print(f"  Deleted: {file_url}")
        deleted += 1
    except Exception:
        pass  # file doesn't exist in this folder, skip

print(f"\nDone. Deleted {deleted} file(s).")
