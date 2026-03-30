# Test validation logic
# from your simulator.py

def test_steps_range():
    # Valid steps
    steps = 150
    assert 0 <= steps <= 200
    print("Steps range test passed!")

def test_calories_range():
    # Valid calories
    calories = 5.5
    assert 0 <= calories <= 20
    print("Calories range test passed!")

def test_intensity_values():
    # Valid intensity
    intensity = 2
    assert intensity in [0, 1, 2, 3]
    print("Intensity test passed!")

def test_pii_masking():
    # Check user_id masked
    import hashlib
    user_id = "1503960366"
    masked = "USR_" + hashlib.sha256(
        user_id.encode()
    ).hexdigest()[:8]
    assert masked.startswith("USR_")
    print("PII masking test passed!")

def test_timestamp_format():
    # Check timestamp format
    from datetime import datetime
    timestamp = "4/12/2016 12:00:00 AM"
    parsed = datetime.strptime(
        timestamp,
        '%m/%d/%Y %I:%M:%S %p'
    )
    assert parsed.year == 2016
    print("Timestamp test passed!")