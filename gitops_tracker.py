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
TARGET_CLUSTER_COLUMN = "OCP Dev"
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


def normalize_key(namespace: str, deployment: str) -> Tuple[str, str]:
    """Normalize lookup keys to avoid mismatches from case or extra spaces."""
    return (namespace.strip().lower(), deployment.strip().lower())


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
    """Convert only GitOps-enabled OpenShift deployments into normalized records."""
    records: List[DeploymentRecord] = []

    for item in items:
        metadata = item.get("metadata", {})
        labels = metadata.get("labels") or {}
        gitops_enabled = str(labels.get(gitops_label_key, "")).lower() == gitops_label_value.lower()

        if not gitops_enabled:
            continue

        records.append(
            DeploymentRecord(
                namespace=metadata.get("namespace", "").strip(),
                deployment=metadata.get("name", "").strip(),
                status=GITOPS_STATUS,
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
) -> Tuple[Dict[Tuple[str, str], Dict[str, object]], int]:
    """Load existing rows into a lookup keyed by namespace and deployment name."""
    try:
        values = worksheet.get_all_values()
    except (APIError, GSpreadException) as exc:
        print(f"Failed to read data from Google Sheet: {exc}", file=sys.stderr)
        sys.exit(1)

    namespace_index = header_map[NAMESPACE_HEADER] - 1
    deployment_index = header_map[DEPLOYMENT_HEADER] - 1
    target_cluster_index = header_map[TARGET_CLUSTER_COLUMN] - 1
    number_index = header_map.get("NO", 0) - 1

    records: Dict[Tuple[str, str], Dict[str, object]] = {}
    max_number = 0

    for row_number, row in enumerate(values[1:], start=2):
        namespace = row[namespace_index].strip() if len(row) > namespace_index else ""
        deployment = row[deployment_index].strip() if len(row) > deployment_index else ""
        target_value = row[target_cluster_index].strip() if len(row) > target_cluster_index else ""
        number_value = row[number_index].strip() if number_index >= 0 and len(row) > number_index else ""

        if number_value:
            try:
                max_number = max(max_number, int(float(number_value)))
            except ValueError:
                pass

        if not namespace or not deployment:
            continue

        records[normalize_key(namespace, deployment)] = {
            "row_number": row_number,
            "target_value": target_value,
        }

    return records, max_number


def build_new_row(record: DeploymentRecord, header_map: Dict[str, int], sequence_number: int) -> List[str]:
    """Create a new row matching the existing sheet structure."""
    max_column = max(header_map.values())
    row = [""] * max_column
    excluded_columns = set()

    if "NO" in header_map:
        row[header_map["NO"] - 1] = str(sequence_number)
        excluded_columns.add(header_map["NO"] - 1)

    row[header_map[NAMESPACE_HEADER] - 1] = record.namespace
    row[header_map[DEPLOYMENT_HEADER] - 1] = record.deployment
    excluded_columns.add(header_map[NAMESPACE_HEADER] - 1)
    excluded_columns.add(header_map[DEPLOYMENT_HEADER] - 1)

    if "BIA PRIORITAS" in header_map:
        excluded_columns.add(header_map["BIA PRIORITAS"] - 1)

    for column_index in range(max_column):
        if column_index in excluded_columns:
            continue
        row[column_index] = NOT_DEPLOYED_VALUE

    row[header_map[TARGET_CLUSTER_COLUMN] - 1] = record.status
    return row


def sync_records(
    worksheet: gspread.Worksheet,
    deployments: List[DeploymentRecord],
    existing_records: Dict[Tuple[str, str], Dict[str, object]],
    header_map: Dict[str, int],
    max_number: int,
    total_deployments: int,
) -> Tuple[int, int, List[str], List[str]]:
    """Insert new rows and update the current cluster column for existing rows."""
    rows_to_add: List[List[str]] = []
    cells_to_update: List[gspread.Cell] = []
    added_count = 0
    updated_count = 0
    added_items: List[str] = []
    updated_items: List[str] = []
    target_cluster_column = header_map[TARGET_CLUSTER_COLUMN]

    for record in deployments:
        key = normalize_key(record.namespace, record.deployment)
        current = existing_records.get(key)

        if current is None:
            rows_to_add.append(build_new_row(record, header_map, max_number + added_count + 1))
            added_count += 1
            added_items.append("{0}/{1}".format(record.namespace, record.deployment))
            continue

        if current.get("target_value", "") == record.status:
            continue

        row_number = int(current["row_number"])
        cells_to_update.append(gspread.Cell(row=row_number, col=target_cluster_column, value=record.status))
        updated_count += 1
        updated_items.append("{0}/{1}".format(record.namespace, record.deployment))

    try:
        if rows_to_add:
            template_row_number = max(2, len(worksheet.get_all_values()))
            worksheet.append_rows(rows_to_add, value_input_option="RAW")
            apply_new_row_formatting(worksheet, header_map, len(rows_to_add), template_row_number)

        if cells_to_update:
            worksheet.update_cells(cells_to_update, value_input_option="RAW")
    except (APIError, GSpreadException) as exc:
        print(f"Failed to update Google Sheet: {exc}", file=sys.stderr)
        sys.exit(1)

    return added_count, updated_count, added_items, updated_items


def apply_new_row_formatting(
    worksheet: gspread.Worksheet,
    header_map: Dict[str, int],
    new_row_count: int,
    template_row_number: int,
) -> None:
    """Copy format and data validation from an existing row to newly appended rows."""
    if new_row_count <= 0:
        return

    try:
        end_column_index = max(header_map.values())
        last_row = len(worksheet.get_all_values())
        start_row_index = last_row - new_row_count
        end_row_index = last_row
        source_start_index = max(template_row_number - 1, 1)
        source_end_index = source_start_index + 1

        worksheet.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "copyPaste": {
                            "source": {
                                "sheetId": worksheet.id,
                                "startRowIndex": source_start_index,
                                "endRowIndex": source_end_index,
                                "startColumnIndex": 0,
                                "endColumnIndex": end_column_index,
                            },
                            "destination": {
                                "sheetId": worksheet.id,
                                "startRowIndex": start_row_index,
                                "endRowIndex": end_row_index,
                                "startColumnIndex": 0,
                                "endColumnIndex": end_column_index,
                            },
                            "pasteType": "PASTE_FORMAT",
                            "pasteOrientation": "NORMAL",
                        }
                    },
                    {
                        "copyPaste": {
                            "source": {
                                "sheetId": worksheet.id,
                                "startRowIndex": source_start_index,
                                "endRowIndex": source_end_index,
                                "startColumnIndex": 0,
                                "endColumnIndex": end_column_index,
                            },
                            "destination": {
                                "sheetId": worksheet.id,
                                "startRowIndex": start_row_index,
                                "endRowIndex": end_row_index,
                                "startColumnIndex": 0,
                                "endColumnIndex": end_column_index,
                            },
                            "pasteType": "PASTE_DATA_VALIDATION",
                            "pasteOrientation": "NORMAL",
                        }
                    },
                ]
            }
        )
    except (APIError, GSpreadException) as exc:
        print(f"Failed to copy row formatting or dropdowns: {exc}", file=sys.stderr)
        sys.exit(1)


def apply_table_layout(worksheet: gspread.Worksheet, header_map: Dict[str, int]) -> None:
    """Apply a table-like layout by freezing the header row and updating the filter range."""
    try:
        values = worksheet.get_all_values()
        row_count = max(len(values), 1)
        column_count = max(header_map.values())

        worksheet.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": worksheet.id,
                                "gridProperties": {
                                    "frozenRowCount": 1,
                                },
                            },
                            "fields": "gridProperties.frozenRowCount",
                        }
                    },
                    {
                        "setBasicFilter": {
                            "filter": {
                                "range": {
                                    "sheetId": worksheet.id,
                                    "startRowIndex": 0,
                                    "endRowIndex": row_count,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": column_count,
                                }
                            }
                        }
                    },
                ]
            }
        )
    except (APIError, GSpreadException) as exc:
        print(f"Failed to apply sheet table layout: {exc}", file=sys.stderr)
        sys.exit(1)


def print_summary(
    total_deployments: int,
    gitops_deployments: int,
    new_rows_added: int,
    rows_updated: int,
    added_items: List[str],
    updated_items: List[str],
) -> None:
    """Print the required sync summary."""
    gitops_percentage = 0.0

    if total_deployments:
        gitops_percentage = (float(gitops_deployments) / float(total_deployments)) * 100.0

    print(f"Total Deployments: {total_deployments}")
    print(f"GitOps Deployments: {gitops_deployments}")
    print("GitOps Percentage: {0:.2f}%".format(gitops_percentage))
    print(f"New Rows Added: {new_rows_added}")
    print(f"Rows Updated: {rows_updated}")

    print("Added Deployments:")
    if added_items:
        for item in added_items:
            print(" - {0}".format(item))
    else:
        print(" - None")

    print("Updated Deployments:")
    if updated_items:
        for item in updated_items:
            print(" - {0}".format(item))
    else:
        print(" - None")


def main() -> None:
    """Run the full sync process."""
    validate_config()

    deployment_items = run_oc_get_deployments()
    total_deployments = len(deployment_items)
    deployments = build_deployment_records(deployment_items, GITOPS_LABEL_KEY, GITOPS_LABEL_VALUE)

    client = get_gspread_client(SERVICE_ACCOUNT_FILE)
    worksheet = open_worksheet(client, SPREADSHEET_ID, WORKSHEET_NAME)
    header_map = get_header_map(worksheet)
    existing_records, max_number = load_sheet_records(worksheet, header_map)

    new_rows_added, rows_updated, added_items, updated_items = sync_records(
        worksheet,
        deployments,
        existing_records,
        header_map,
        max_number,
        total_deployments,
    )
    apply_table_layout(worksheet, header_map)
    gitops_deployments = len(deployments)

    print_summary(
        total_deployments=total_deployments,
        gitops_deployments=gitops_deployments,
        new_rows_added=new_rows_added,
        rows_updated=rows_updated,
        added_items=added_items,
        updated_items=updated_items,
    )


if __name__ == "__main__":
    main()
