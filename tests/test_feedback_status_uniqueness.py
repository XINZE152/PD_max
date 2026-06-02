"""Test: feedback_status uniqueness constraint — code-level verification."""
import inspect
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_migration_sql():
    print("Test 1: Migration SQL correctness")
    from app.database import ensure_ai_detection_history_feedback_status_column
    source = inspect.getsource(ensure_ai_detection_history_feedback_status_column)
    assert "feedback_status" in source
    assert "ALTER TABLE ai_detection_history ADD COLUMN" in source
    assert "idx_ai_hist_feedback" in source
    print("  PASS")


def test_table_definition():
    print("Test 2: TABLE_STATEMENTS contains feedback_status + index")
    from app.database import TABLE_STATEMENTS
    found_col = found_idx = False
    for stmt in TABLE_STATEMENTS:
        if "ai_detection_history" in stmt:
            if "feedback_status" in stmt:
                found_col = True
            if "idx_ai_hist_feedback" in stmt:
                found_idx = True
    assert found_col, "feedback_status column not found"
    assert found_idx, "idx_ai_hist_feedback not found"
    print("  PASS")


def test_helper_functions_exist():
    print("Test 3: history_db exports get/mark/clear")
    from app.ai_detection import history_db as hdb
    assert hasattr(hdb, "get_feedback_status")
    assert hasattr(hdb, "mark_feedback_status")
    assert hasattr(hdb, "clear_feedback_status")
    assert callable(hdb.get_feedback_status)
    assert callable(hdb.mark_feedback_status)
    assert callable(hdb.clear_feedback_status)
    print("  PASS")


def test_helper_function_signatures():
    print("Test 4: Helper function signatures")
    from app.ai_detection.history_db import get_feedback_status, mark_feedback_status, clear_feedback_status

    sig_get = inspect.signature(get_feedback_status)
    assert "task_id" in sig_get.parameters

    sig_mark = inspect.signature(mark_feedback_status)
    assert "task_id" in sig_mark.parameters
    assert "judgment" in sig_mark.parameters

    sig_clear = inspect.signature(clear_feedback_status)
    assert "task_id" in sig_clear.parameters
    print("  PASS")


def test_helper_function_logic():
    print("Test 5: Helper function SQL logic (source inspection)")
    from app.ai_detection.history_db import get_feedback_status, mark_feedback_status, clear_feedback_status

    # get_feedback_status should query async_v3 mode and order by id DESC
    src_get = inspect.getsource(get_feedback_status)
    assert "feedback_status" in src_get
    assert "async_v3" in src_get
    assert "ORDER BY id DESC" in src_get

    # mark_feedback_status should UPDATE async_v3
    src_mark = inspect.getsource(mark_feedback_status)
    assert "feedback_status=%s" in src_mark
    assert "async_v3" in src_mark

    # clear_feedback_status should set NULL
    src_clear = inspect.getsource(clear_feedback_status)
    assert "feedback_status=NULL" in src_clear
    assert "async_v3" in src_clear
    print("  PASS")


def test_imports_in_route():
    print("Test 6: ai_detection.py imports new functions")
    from app.api.v1.routes import ai_detection as route_mod
    assert hasattr(route_mod, "get_feedback_status")
    assert hasattr(route_mod, "mark_feedback_status")
    assert hasattr(route_mod, "clear_feedback_status")
    print("  PASS")


def test_submit_judgment_has_409_check():
    print("Test 7: submit_judgment contains 409 duplicate check")
    from app.api.v1.routes.ai_detection import submit_judgment
    src = inspect.getsource(submit_judgment)
    assert "get_feedback_status" in src
    assert "409" in src
    assert "已标注" in src
    assert "mark_feedback_status" in src
    print("  PASS")


def test_update_feedback_syncs_db():
    print("Test 8: update_feedback syncs feedback_status to DB")
    from app.api.v1.routes.ai_detection import update_feedback
    src = inspect.getsource(update_feedback)
    assert "mark_feedback_status" in src
    assert "task_id" in src
    print("  PASS")


def test_delete_feedback_clears_db():
    print("Test 9: delete_feedback clears feedback_status in DB")
    from app.api.v1.routes.ai_detection import delete_feedback
    src = inspect.getsource(delete_feedback)
    assert "clear_feedback_status" in src
    assert "task_id" in src
    print("  PASS")


def test_confirm_suspicious_syncs_db():
    print("Test 10: confirm_suspicious syncs feedback_status to DB")
    from app.api.v1.routes.ai_detection import confirm_suspicious
    src = inspect.getsource(confirm_suspicious)
    assert "mark_feedback_status" in src
    print("  PASS")


def test_feedback_manager_unchanged():
    print("Test 11: FeedbackManager file operations still work")
    import tempfile
    from pathlib import Path
    from app.ai_detection.feedback_manager import FeedbackManager

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = f"{tmpdir}/cfg.yaml"
        with open(config_path, "w") as f:
            f.write(f"feedback:\n  storage_dir: {tmpdir}/fb\n")

        # Create a dummy image file
        dummy_img = f"{tmpdir}/dummy.jpg"
        with open(dummy_img, "wb") as f:
            f.write(b"fake")

        fb = FeedbackManager(config_path)
        # Test save correct
        e = fb.save_judgment("t1", "correct", dummy_img, note="x")
        assert e["judgment"] == "correct"
        folder1 = Path(e["original_image"]).parent.name
        # Test update: correct → wrong
        u = fb.update_entry(folder1, "wrong")
        assert u["judgment"] == "wrong"
        # Test save suspicious + confirm
        e2 = fb.save_judgment("t2", "suspicious", dummy_img)
        folder2 = Path(e2["original_image"]).parent.name
        c = fb.confirm_suspicious(folder2, "correct")
        assert c is not None and c["judgment"] == "correct"
        # Test delete
        d = fb.delete_entry(folder1)
        assert d is True
    print("  PASS")


def test_list_history_includes_feedback_status():
    print("Test 12: list_ai_detection_history SELECT includes feedback_status")
    from app.ai_detection.history_db import list_ai_detection_history
    src = inspect.getsource(list_ai_detection_history)
    assert "feedback_status" in src
    print("  PASS")


def test_create_tables_calls_migration():
    print("Test 13: create_tables() calls ensure_ai_detection_history_feedback_status_column")
    from app.database import create_tables
    src = inspect.getsource(create_tables)
    assert "ensure_ai_detection_history_feedback_status_column" in src
    print("  PASS")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("AI Detection Feedback Uniqueness — Code Verification")
    print("=" * 60 + "\n")

    tests = [
        test_migration_sql,
        test_table_definition,
        test_helper_functions_exist,
        test_helper_function_signatures,
        test_helper_function_logic,
        test_imports_in_route,
        test_submit_judgment_has_409_check,
        test_update_feedback_syncs_db,
        test_delete_feedback_clears_db,
        test_confirm_suspicious_syncs_db,
        test_feedback_manager_unchanged,
        test_list_history_includes_feedback_status,
        test_create_tables_calls_migration,
    ]

    passed = failed = 0
    for fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed, {len(tests)} total")
    print("=" * 60)
    if failed:
        sys.exit(1)
