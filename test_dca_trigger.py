import sys
import os

# Mock components for testing
class MockLevel:
    def __init__(self, level, price, amount, status):
        self.data = {
            'level': level,
            'price': price,
            'amount': amount,
            'status': status
        }
    def get(self, key, default=None):
        return self.data.get(key, default)
    def __getitem__(self, key):
        return self.data[key]
    def __setitem__(self, key, value):
        self.data[key] = value

def test_trigger_logic(current_price, levels):
    triggered = []
    for level in levels:
        level_num = level.get('level', 1)
        trigger_price = level.get('price', 0)
        allocate_amount = level.get('amount', 0)

        if level_num == 1:
            continue

        # Trigger logic: Price <= Trigger Price AND status == 'pending'
        if current_price <= trigger_price and level.get('status') == 'pending':
            print(f"🔥 TRIGGERED: Level {level_num} hit! Current: {current_price} Target: {trigger_price}")
            triggered.append(level_num)
            level['status'] = 'notified'
        else:
            print(f"--- Skip: Level {level_num} (Price: {trigger_price}, Status: {level.get('status')})")
    
    return triggered

# Test Case 1: Price is below Level 2 but above Level 3
print("Test Case 1: Price $21.55 (Level 2: $22.34, Level 3: $20.40)")
levels1 = [
    MockLevel(1, 24.28, 1000, 'executed'),
    MockLevel(2, 22.34, 1000, 'pending'),
    MockLevel(3, 20.40, 1000, 'pending')
]
res1 = test_trigger_logic(21.55, levels1)
assert 2 in res1
assert 3 not in res1
print("✅ Test Case 1 Passed\n")

# Test Case 2: Price is below Level 2 AND Level 3 (Late Scan)
print("Test Case 2: Price $19.00 (Level 2: $22.34, Level 3: $20.40)")
levels2 = [
    MockLevel(1, 24.28, 1000, 'executed'),
    MockLevel(2, 22.34, 1000, 'pending'),
    MockLevel(3, 20.40, 1000, 'pending')
]
res2 = test_trigger_logic(19.00, levels2)
assert 2 in res2
assert 3 in res2
print("✅ Test Case 2 Passed (Multiple triggers handled)\n")

# Test Case 3: Level already notified
print("Test Case 3: Price $21.55, Level 2 already notified")
levels3 = [
    MockLevel(1, 24.28, 1000, 'executed'),
    MockLevel(2, 22.34, 1000, 'notified'),
    MockLevel(3, 20.40, 1000, 'pending')
]
res3 = test_trigger_logic(21.55, levels3)
assert 2 not in res3
print("✅ Test Case 3 Passed\n")
