import pytest
from server.scoring import (
    get_mw_bucket,
    compute_success_probability,
    flag_phantom,
    compute_workforce
)
import datetime

def test_get_mw_bucket():
    assert get_mw_bucket(10) == "<50"
    assert get_mw_bucket(50) == "50-200"
    assert get_mw_bucket(150) == "50-200"
    assert get_mw_bucket(250) == "200-500"
    assert get_mw_bucket(600) == "500-1000"
    assert get_mw_bucket(1200) == "1000+"

def test_compute_success_probability():
    # Known outcomes
    assert compute_success_probability("Solar", "CAISO", 100, "Operational") == 1.0
    assert compute_success_probability("Wind", "ERCOT", 100, "Withdrawn") == 0.0

    # Base rate logic (Solar=0.14)
    # Adjustments: MW(100) -> +0.03
    # Fresh queue (say 10 days ago) -> +0.04
    now = datetime.datetime.utcnow()
    recent_date = (now - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    
    prob_recent = compute_success_probability(
        technology="Solar", 
        iso="CAISO", 
        capacity_mw=100, 
        status="Active", 
        queue_date=recent_date
    )
    # Base 0.14 + MW 0.03 + Age 0.04 = 0.21
    # Floating point precision might make it 0.21 or so.
    assert prob_recent == pytest.approx(0.21, abs=0.02)
    
    # Suspended penalty
    prob_suspended = compute_success_probability(
        technology="Solar", 
        iso="CAISO", 
        capacity_mw=100, 
        status="Suspended", 
        queue_date=recent_date
    )
    # 0.21 * 0.3 = 0.063 -> rounded to 0.06
    assert prob_suspended == pytest.approx(0.06, abs=0.02)

def test_flag_phantom():
    now = datetime.datetime.utcnow()
    old_date = (now - datetime.timedelta(days=1000)).strftime("%Y-%m-%d")
    recent_date = (now - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    
    # Not active -> not phantom
    assert flag_phantom("Operational", 0.05, old_date) == False
    
    # High success prob -> not phantom
    assert flag_phantom("Active", 0.25, old_date) == False
    
    # Low success, but recently entered -> not phantom
    assert flag_phantom("Active", 0.05, recent_date) == False
    
    # Low success, old date (>730 days) -> phantom
    assert flag_phantom("Active", 0.05, old_date) == True

def test_compute_workforce():
    res = compute_workforce("Solar", 100)
    assert "workforce_total" in res
    assert "workforce_electricians" in res
    assert "construction_years" in res
    
    # 100 MW solar * 0.8 base * variation -> ~80
    assert 60 <= res["workforce_total"] <= 100
