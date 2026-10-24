import hashlib
import json
import zipfile
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from trade_data.operations.nse_authority import (
    NSE_SOURCE,
    SOURCE_COLUMNS,
    prepare_nse_batch,
    verify_nse_dataset,
)

DAY = date(2026, 10, 23)


@pytest.fixture
def source(tmp_path):
    raw = pd.DataFrame(
        [
            {
                "TckrSymb": "TEST",
                "ISIN": "INE123A01011",
                "SctySrs": "EQ",
                "OpnPric": 100.0,
                "HghPric": 105.0,
                "LwPric": 99.0,
                "ClsPric": 104.0,
                "TtlTradgVol": 123456789,
            }
        ]
    )
    archive = tmp_path / f"{DAY}.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("original.csv", raw.to_csv(index=False))
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    url = "https://nsearchives.nseindia.com/content/cm/original.zip"
    archive.with_suffix(".json").write_text(json.dumps({"url": url, "sha256": digest}))
    f = raw.rename(columns=SOURCE_COLUMNS)
    f = f.assign(
        symbol="TEST.NS",
        current_isin="INE123A01011",
        exchange="NSE",
        date=DAY,
        source=NSE_SOURCE,
        price_basis="as_traded",
        volume_basis="exchange_reported_shares",
        evidence_url=url,
        evidence_sha256=digest,
    )
    return tmp_path, f


def verify(folder, f):
    path = folder / "data.parquet"
    f.to_parquet(path, index=False)
    return verify_nse_dataset(
        path,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        archives=folder,
        eligible_sessions={DAY},
    )


def test_source_exactness_and_integer_volume_survive_batch_translation(source):
    folder, f = source
    f = verify(folder, f)
    identities = {
        "TEST.NS": {
            "canonical_instrument_id": "eq_test",
            "provider_instrument_key": "YF|TEST.NS",
            "source_identity": "INE123A01011",
        }
    }
    b = prepare_nse_batch(
        f,
        identities=identities,
        settings=SimpleNamespace(object_store_write_enabled=False),
        database_engine=None,
        run_id="test",
        retrieved_at=datetime(2026, 10, 24, tzinfo=UTC),
        eligible_sessions={DAY},
    )
    assert b.validation.valid
    assert b.candles[0].volume == 123456789
    assert b.candles[0].provider == NSE_SOURCE
    assert b.frame.source.eq(NSE_SOURCE).all()
    assert b.frame.instrument_key.tolist() == ["YF|TEST.NS"]
    assert "adjusted_close" not in b.frame
    identities["TEST.NS"]["source_identity"] = "OTHER_SECURITY"
    with pytest.raises(ValueError, match="identity"):
        prepare_nse_batch(
            f,
            identities=identities,
            settings=SimpleNamespace(object_store_write_enabled=False),
            database_engine=None,
            run_id="test",
            retrieved_at=datetime(2026, 10, 24, tzinfo=UTC),
            eligible_sessions={DAY},
        )


@pytest.mark.parametrize("field,value", [("open", 101.0), ("volume", 123456788)])
def test_well_formed_but_wrong_exchange_values_are_rejected(source, field, value):
    folder, f = source
    f.loc[0, field] = value
    with pytest.raises(ValueError, match="differs from original"):
        verify(folder, f)


def test_tampered_archive_and_duplicate_rows_are_rejected(source):
    folder, f = source
    with pytest.raises(ValueError, match="duplicate"):
        verify(folder, pd.concat([f, f]))
    (folder / f"{DAY}.zip").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum changed"):
        verify(folder, f)


def test_missing_session_and_wrong_price_basis_are_rejected(source):
    folder, f = source
    f["price_basis"] = "adjusted"
    with pytest.raises(ValueError, match="basis"):
        verify(folder, f)
    f["price_basis"] = "as_traded"
    path = folder / "data.parquet"
    f.to_parquet(path)
    with pytest.raises(ValueError, match="calendar"):
        verify_nse_dataset(
            path,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            archives=folder,
            eligible_sessions={DAY, date(2026, 10, 22)},
        )
