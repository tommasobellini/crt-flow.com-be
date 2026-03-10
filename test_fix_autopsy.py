
def test_grouping_logic():
    # Mock data
    expired_signals_updates = [
        {"id": 1, "result": "WIN", "exit_reason": "Clean Snipe"},
        {"id": 2, "result": "WIN", "exit_reason": "Clean Snipe"},
        {"id": 3, "result": "LOSS", "exit_reason": "Stop Hunt (Wicked Out)"},
        {"id": 4, "result": "WIN", "exit_reason": "Struggle Hit (Almost Stopped)"},
    ]
    
    # Logic to test
    updates_by_group = {}
    for u in expired_signals_updates:
        if 'result' in u:
            res = u['result']
            reason_txt = u.get('exit_reason', 'Standard Exit')
            group_key = (res, reason_txt)
            
            if group_key not in updates_by_group: 
                updates_by_group[group_key] = []
            updates_by_group[group_key].append(u['id'])
            
    # Verify results
    expected = {
        ("WIN", "Clean Snipe"): [1, 2],
        ("LOSS", "Stop Hunt (Wicked Out)"): [3],
        ("WIN", "Struggle Hit (Almost Stopped)"): [4]
    }
    
    print(f"Updates by group: {updates_by_group}")
    assert updates_by_group == expected
    print("Verification SUCCESS: Grouping logic preserves exit_reason.")

if __name__ == "__main__":
    test_grouping_logic()
