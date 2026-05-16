# bulk

NAVER search ad operations dashboard and collector workflow.

## Account master file

The account master workbook is not committed because it contains account and customer information. In GitHub Actions, store the workbook as the repository secret `ACCOUNT_MASTER_XLSX_B64`; the collectors restore it to `account_master.xlsx` at runtime.

PowerShell example:

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("account_master.xlsx"))
```

## Checks

```bash
python smoke_check.py --with-help --with-regression
```

## PC/mobile device data

If NAVER omits the PC/mobile split report, the collector now keeps the total as `UNSEGMENTED`. The Streamlit UI displays this bucket as an unsegmented total, so totals remain visible instead of disappearing.

## Retired media placement data

The media placement page and `fact_media_daily` collection path are retired. Existing database data can be checked with:

```bash
python cleanup_media_data.py
```

Run the cleanup only after confirming the dry-run count:

```bash
python cleanup_media_data.py --execute
```
