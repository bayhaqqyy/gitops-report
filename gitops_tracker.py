#!/usr/bin/env python3
"""
Synchronize OpenShift deployment GitOps status with the all-deployment sheet.

Update the configuration values below before running the script.
"""

import json
import subprocess
import sys
from typing import Dict, List, Tuple

import gspread
from google.auth.exceptions import GoogleAuthError
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, GSpreadException, SpreadsheetNotFound, WorksheetNotFound


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Script configuration.
SERVICE_ACCOUNT_FILE = "/home/ADMINISTRATOR/ivtsvc/gitops-report/service-account.json"
SPREADSHEET_ID = "1DvS8LsjNc5Lury4GiDPdyfmTX8tlTGLtka1xL12pB8w"
WORKSHEET_NAME = "all-deployment"
TARGET_CLUSTER_COLUMN = "OCP DEV"
GITOPS_LABEL_KEY = "gitops"
GITOPS_LABEL_VALUE = "true"

# Sheet constants based on the existing workbook structure.
NAMESPACE_HEADER = "Namespace"
DEPLOYMENT_HEADER = "Deployment / Deployment Config"
NOT_DEPLOYED_VALUE = "Not Deployed"
GITOPS_STATUS = "GitOps"
STANDARD_DEPLOYMENT_STATUS = "Standard Deployment"


class DeploymentRecord:
    """Simple deployment record container compatible with older Python versions."""

    def __init__(self, namespace: str, deployment: str, status: str) -> None:
        self.namespace = namespace
        self.deployment = deployment
        self.status = status


def validate_config() -> None:
    """Validate required script configuration before execution."""
    required_values = {
        "SERVICE_ACCOUNT_FILE": SERVICE_ACCOUNT_FILE,
        "SPREADSHEET_ID": SPREADSHEET_ID,
        "WORKSHEET_NAME": WORKSHEET_NAME,
        "TARGET_CLUSTER_COLUMN": TARGET_CLUSTER_COLUMN,
        "GITOPS_LABEL_KEY": GITOPS_LABEL_KEY,
        "GITOPS_LABEL_VALUE": GITOPS_LABEL_VALUE,
    }

    for config_name, value in required_values.items():
        if str(value).strip():
            continue

        print(f"Missing required configuration value: {config_name}", file=sys.stderr)
        sys.exit(1)


def run_oc_get_deployments() -> List[dict]:
    """Run the oc command and return deployment items from the cluster."""
    command = ["oc", "get", "deploy", "-A", "-o", "json"]

    try:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
    except FileNotFoundError:
        print("The 'oc' command was not found. Ensure it is installed and available in PATH.", file=sys.stderr)
        sys.exit(1)
    except OSError as exc:
        print(f"Failed to execute oc command: {exc}", file=sys.stderr)
        sys.exit(1)

    if result.returncode != 0:
        error_message = result.stderr.strip() or "Unknown error"
        print(f"oc command failed: {error_message}", file=sys.stderr)
        sys.exit(1)

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"Failed to parse oc JSON output: {exc}", file=sys.stderr)
        sys.exit(1)

    return payload.get("items", [])


def build_deployment_records(items: List[dict], gitops_label_key: str, gitops_label_value: str) -> List[DeploymentRecord]:
    """Convert OpenShift deployment JSON items into normalized records."""
    records: List[DeploymentRecord] = []

    for item in items:
        metadata = item.get("metadata", {})
        labels = metadata.get("labels") or {}
        gitops_enabled = str(labels.get(gitops_label_key, "")).lower() == gitops_label_value.lower()

        records.append(
            DeploymentRecord(
                namespace=metadata.get("namespace", "").strip(),
                deployment=metadata.get("name", "").strip(),
                status=GITOPS_STATUS if gitops_enabled else STANDARD_DEPLOYMENT_STATUS,
            )
        )

    records.sort(key=lambda record: (record.namespace, record.deployment))
    return records


def get_gspread_client(service_account_file: str) -> gspread.Client:
    """Authenticate with Google using a service account file."""
    try:
        credentials = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
        return gspread.authorize(credentials)
    except FileNotFoundError:
        print(f"Service account file not found: {service_account_file}", file=sys.stderr)
        sys.exit(1)
    except (GoogleAuthError, ValueError) as exc:
        print(f"Google authentication failed: {exc}", file=sys.stderr)
        sys.exit(1)


def open_worksheet(client: gspread.Client, spreadsheet_id: str, worksheet_name: str) -> gspread.Worksheet:
    """Open the target worksheet from the spreadsheet."""
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        return spreadsheet.worksheet(worksheet_name)
    except SpreadsheetNotFound:
        print(
            "Google Sheet not found or not shared with the service account: {0}".format(spreadsheet_id),
            file=sys.stderr,
        )
        sys.exit(1)
    except WorksheetNotFound:
        print(f"Worksheet not found: {worksheet_name}", file=sys.stderr)
        sys.exit(1)
    except (APIError, GSpreadException) as exc:
        print(f"Failed to open Google Sheet: {exc}", file=sys.stderr)
        sys.exit(1)


def get_header_map(worksheet: gspread.Worksheet) -> Dict[str, int]:
    """Return a mapping of header name to 1-based column index."""
    try:
        headers = worksheet.row_values(1)
    except (APIError, GSpreadException) as exc:
        print(f"Failed to read sheet headers: {exc}", file=sys.stderr)
        sys.exit(1)

    header_map = {header.strip(): index for index, header in enumerate(headers, start=1) if header.strip()}

    for required_header in (NAMESPACE_HEADER, DEPLOYMENT_HEADER, TARGET_CLUSTER_COLUMN):
        if required_header in header_map:
            continue

        print(f"Required header not found in sheet: {required_header}", file=sys.stderr)
        sys.exit(1)

    return header_map


def load_sheet_records(
    worksheet: gspread.Worksheet,
    header_map: Dict[str, int],
) -> Dict[Tuple[str, str], Dict[str, object]]:
    """Load existing rows into a lookup keyed by namespace and deployment name."""
    try:
        values = worksheet.get_all_values()
    except (APIError, GSpreadException) as exc:
        print(f"Failed to read data from Google Sheet: {exc}", file=sys.stderr)
        sys.exit(1)

    namespace_index = header_map[NAMESPACE_HEADER] - 1
    deployment_index = header_map[DEPLOYMENT_HEADER] - 1
    target_cluster_index = header_map[TARGET_CLUSTER_COLUMN] - 1

    records: Dict[Tuple[str, str], Dict[str, object]] = {}

    for row_number, row in enumerate(values[1:], start=2):
        namespace = row[namespace_index].strip() if len(row) > namespace_index else ""
        deployment = row[deployment_index].strip() if len(row) > deployment_index else ""
        target_value = row[target_cluster_index].strip() if len(row) > target_cluster_index else ""

        if not namespace or not deployment:
            continue

        records[(namespace, deployment)] = {
            "row_number": row_number,
            "target_value": target_value,
        }

    return records


def build_new_row(record: DeploymentRecord, header_map: Dict[str, int]) -> List[str]:
    """Create a new row matching the existing sheet structure."""
    max_column = max(header_map.values())
    row = [""] * max_column

    row[header_map[NAMESPACE_HEADER] - 1] = record.namespace
    row[header_map[DEPLOYMENT_HEADER] - 1] = record.deployment

    for header, column_index in header_map.items():
        if header in (NAMESPACE_HEADER, DEPLOYMENT_HEADER, "NO", "BIA PRIORITAS"):
            continue

        row[column_index - 1] = NOT_DEPLOYED_VALUE

    row[header_map[TARGET_CLUSTER_COLUMN] - 1] = record.status
    return row


def sync_records(
    worksheet: gspread.Worksheet,
    deployments: List[DeploymentRecord],
    existing_records: Dict[Tuple[str, str], Dict[str, object]],
    header_map: Dict[str, int],
) -> Tuple[int, int]:
    """Insert new rows and update the current cluster column for existing rows."""
    rows_to_add: List[List[str]] = []
    cells_to_update: List[gspread.Cell] = []
    added_count = 0
    updated_count = 0
    target_cluster_column = header_map[TARGET_CLUSTER_COLUMN]

    for record in deployments:
        key = (record.namespace, record.deployment)
        current = existing_records.get(key)

        if current is None:
            rows_to_add.append(build_new_row(record, header_map))
            added_count += 1
            continue

        if current.get("target_value", "") == record.status:
            continue

        row_number = int(current["row_number"])
        cells_to_update.append(gspread.Cell(row=row_number, col=target_cluster_column, value=record.status))
        updated_count += 1

    try:
        if rows_to_add:
            worksheet.append_rows(rows_to_add, value_input_option="RAW")

        if cells_to_update:
            worksheet.update_cells(cells_to_update, value_input_option="RAW")
    except (APIError, GSpreadException) as exc:
        print(f"Failed to update Google Sheet: {exc}", file=sys.stderr)
        sys.exit(1)

    return added_count, updated_count


def print_summary(total_deployments: int, gitops_deployments: int, new_rows_added: int, rows_updated: int) -> None:
    """Print the required sync summary."""
    print(f"Total Deployments: {total_deployments}")
    print(f"GitOps Deployments: {gitops_deployments}")
    print(f"New Rows Added: {new_rows_added}")
    print(f"Rows Updated: {rows_updated}")


def main() -> None:
    """Run the full sync process."""
    validate_config()

    deployment_items = run_oc_get_deployments()
    deployments = build_deployment_records(deployment_items, GITOPS_LABEL_KEY, GITOPS_LABEL_VALUE)

    client = get_gspread_client(SERVICE_ACCOUNT_FILE)
    worksheet = open_worksheet(client, SPREADSHEET_ID, WORKSHEET_NAME)
    header_map = get_header_map(worksheet)
    existing_records = load_sheet_records(worksheet, header_map)

    new_rows_added, rows_updated = sync_records(worksheet, deployments, existing_records, header_map)
    gitops_deployments = sum(1 for deployment in deployments if deployment.status == GITOPS_STATUS)

    print_summary(
        total_deployments=len(deployments),
        gitops_deployments=gitops_deployments,
        new_rows_added=new_rows_added,
        rows_updated=rows_updated,
    )


if __name__ == "__main__":
    main()
