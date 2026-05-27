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

## Meta Ads collection

Meta campaign/ad performance can be synced into the same dashboard tables with:

```bash
python collector_meta.py --date 2026-05-15 --ad_account_id act_143436265335363
```

Required environment variables:

- `DATABASE_URL`
- `META_ACCESS_TOKEN`

For scheduled GitHub Actions, add `META_ACCESS_TOKEN` as a repository secret. Meta accounts can be listed in `account_master.xlsx` with `platform=meta` and `meta_ad_account_id`, or supplied manually with `--ad_account_id`.

## Google Ads collection

Google Ads campaign/ad performance can be synced into the same dashboard tables with:

```bash
python collector_google_ads.py --start 2026-05-01 --end 2026-05-26 --customer_id 276-154-7013 --account_name 핵이득마켓 --manager 승훈
```

Required environment variables:

- `DATABASE_URL`
- `GOOGLE_ADS_DEVELOPER_TOKEN`
- `GOOGLE_ADS_CLIENT_ID`
- `GOOGLE_ADS_CLIENT_SECRET`
- `GOOGLE_ADS_REFRESH_TOKEN`, unless a per-account `refresh_token` is stored in `platform_credentials`

For scheduled GitHub Actions, add the Google Ads credentials as repository secrets. Dashboard account links can be edited in Settings > Dashboard management > Platform account connections; active `google` rows are collected automatically by workflow `08 Google Ads 동기화`.

## Retired media placement data

The media placement page and `fact_media_daily` collection path are retired. Existing database data can be checked with:

```bash
python cleanup_media_data.py
```

Run the cleanup only after confirming the dry-run count:

```bash
python cleanup_media_data.py --execute
```
