import pytest
import datetime
import pandas as pd
from server.ingest.gridstatus_puller import (
    normalize_technology,
    normalize_status,
    normalize_state,
    generate_project_url,
    safe_str,
    safe_float,
    safe_date
)

def test_normalize_technology():
    assert normalize_technology("Solar PV") == "Solar"
    assert normalize_technology("BESS") == "Battery Storage"
    assert normalize_technology("Co-located Storage") == "Battery Storage"
    assert normalize_technology("Onshore Wind") == "Wind"
    assert normalize_technology("Wind + Storage") == "Hybrid"
    assert normalize_technology("Gas CT") == "Natural Gas"
    assert normalize_technology("Unknown") == "Other"
    assert normalize_technology(None) == "Other"

def test_normalize_status():
    assert normalize_status("In Service") == "Operational"
    assert normalize_status("Commercial Operation") == "Operational"
    assert normalize_status("Active") == "Active"
    assert normalize_status("In Queue") == "Active"
    assert normalize_status("Withdrawn") == "Withdrawn"
    assert normalize_status("Deactivated") == "Withdrawn"
    assert normalize_status("Suspended") == "Suspended"
    assert normalize_status(None) == "Active"

def test_normalize_state():
    assert normalize_state("CALIFORNIA") == "CA"
    assert normalize_state("TEX") == "TX"
    assert normalize_state("NEW YORK") == "NY"
    assert normalize_state("NY") == "NY"
    assert normalize_state("ZZ") == None
    assert normalize_state(None) == None

def test_generate_project_url():
    assert generate_project_url("CAISO", "1234") == "https://www.google.com/search?q=%22CAISO%22+%221234%22"
    assert generate_project_url("ERCOT", "9999", "Oasis Solar") == "https://www.google.com/search?q=%22ERCOT%22+%229999%22+%22Oasis+Solar%22"
    assert generate_project_url("PJM", "Z1-123", None, "NextEra") == "https://www.google.com/search?q=%22PJM%22+%22Z1-123%22+%22NextEra%22"
    assert generate_project_url("MISO", None) == ""

def test_type_safeties():
    assert safe_str(pd.NA) is None
    assert safe_str(" hello ") == "hello"
    
    assert safe_float(12.5) == 12.5
    assert safe_float("12.5") == 12.5
    assert safe_float("bad") is None
    assert safe_float(None) is None
    
    # Test date handling
    d = datetime.date(2025, 1, 1)
    assert safe_date(d) == "2025-01-01"
    assert safe_date("2025-01-01 12:00:00") == "2025-01-01"
    assert safe_date(pd.NaT) is None
