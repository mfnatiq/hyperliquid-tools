from logging import Logger
import pandas as pd


def process_ledger_bridge_operations(
    entries: list,
    queried_address: str,
    unit_token_mappings: dict[str, tuple[str, int]],
    logger: Logger,
) -> pd.DataFrame | None:
    """
    process raw userNonFundingLedgerUpdates entries into a bridge df compatible
    with create_bridge_summary

    direction:
        deposit: destination == queried_address
        withdraw: user == queried_address

    only includes tokens present in unit_token_mappings (i.e. unit bridge tokens)
    amounts are already normalised (not in wei); usdcValue gives USD directly
    """
    if not entries:
        return None

    unit_tokens = {token_name.upper() for token_name, _ in unit_token_mappings.values()}

    spot_transfers = [
        e for e in entries
        if e.get('delta', {}).get('type') == 'spotTransfer'
        and (e.get('delta', {}).get('token') or '').upper() in unit_tokens
    ]
    if not spot_transfers:
        return None

    addr_lower = queried_address.lower()

    rows = []
    for e in spot_transfers:
        d = e['delta']

        dest = (d.get('destination') or '').lower()
        user = (d.get('user') or '').lower()

        if dest == addr_lower:
            direction = 'Deposit'
        elif user == addr_lower:
            direction = 'Withdraw'
        else:
            logger.warning(f'skipping ledger entry where queried address is neither user nor destination: {e}')
            continue

        amount = d.get('amount')
        usdc_value = d.get('usdcValue')
        if amount is None or usdc_value is None:
            continue

        rows.append({
            'opCreatedAt': pd.to_datetime(e['time'], unit='ms', utc=True),
            'asset': d.get('token', '').lower(),
            'direction': direction,
            'amount_formatted': float(amount),
            'amount_usd': float(usdc_value),
        })

    if not rows:
        return None

    return pd.DataFrame(rows)


def create_bridge_summary(df: pd.DataFrame):
    """
    create bridge transaction summary by asset
    """
    if df is None or df.empty:
        return None, ""

    # group by asset and direction
    summary = df.groupby(['asset', 'direction']).agg({
        'amount_formatted': 'sum',
        'amount_usd': 'sum',
        'opCreatedAt': ['min', 'max', 'count']
    })

    # flatten column names
    summary.columns = [
        'Volume', 'Volume (USD)', 'First Txn', 'Last Txn', 'Count']
    summary = summary.reset_index()

    # pivot numeric data with fill_value=0
    pivot_numeric = summary.pivot_table(
        index='asset',
        columns='direction',
        values=['Volume', 'Volume (USD)', 'Count'],
        fill_value=0,
    )

    # pivot datetime data with no fill_value (uses NaT by default)
    pivot_datetime = summary.pivot_table(
        index='asset',
        columns='direction',
        values=['First Txn', 'Last Txn']
    )

    # flatten column names for both pivot tables
    pivot_numeric.columns = [
        f'{col[0]} {col[1]}' for col in pivot_numeric.columns]
    pivot_datetime.columns = [
        f'{col[0]} {col[1]}' for col in pivot_datetime.columns]

    # combine the two pivot tables
    pivot_summary = pivot_numeric.join(pivot_datetime)

    result_data = []
    assets = pivot_summary.index.tolist()

    for asset in assets:
        deposit_volume = pivot_summary.get('Volume Deposit', {}).get(asset, 0)
        deposit_volume_usd = pivot_summary.get(
            'Volume (USD) Deposit', {}).get(asset, 0)
        deposit_first = pivot_summary.get('First Txn Deposit', {}).get(asset)
        deposit_last = pivot_summary.get('Last Txn Deposit', {}).get(asset)
        deposit_count = pivot_summary.get(f'Count Deposit', {}).get(asset, 0)

        withdraw_volume = pivot_summary.get(
            'Volume Withdraw', {}).get(asset, 0)
        withdraw_volume_usd = pivot_summary.get(
            'Volume (USD) Withdraw', {}).get(asset, 0)
        withdraw_first = pivot_summary.get('First Txn Withdraw', {}).get(asset)
        withdraw_last = pivot_summary.get('Last Txn Withdraw', {}).get(asset)
        withdraw_count = pivot_summary.get('Count Withdraw', {}).get(asset, 0)

        # calculate overall first and last transaction
        dates = [d for d in [deposit_first, deposit_last,
                             withdraw_first, withdraw_last] if pd.notna(d)]
        overall_first = min(dates) if dates else pd.NaT
        overall_last = max(dates) if dates else pd.NaT

        total_volume = deposit_volume + withdraw_volume
        total_volume_usd = deposit_volume_usd + withdraw_volume_usd
        total_count = deposit_count + withdraw_count

        result_data.append({
            'Asset': asset.upper(),
            'Deposit': deposit_volume,
            'Deposit (USD)': deposit_volume_usd,
            'Withdraw': withdraw_volume,
            'Withdraw (USD)': withdraw_volume_usd,
            'Total': total_volume,
            'Total (USD)': total_volume_usd,
            'Total Transactions': int(total_count),
            'First Transaction': overall_first,
            'Last Transaction': overall_last,
        })

    result_df = pd.DataFrame(result_data).sort_values(
        'Total (USD)', ascending=False)

    # find top bridged asset
    top_asset = result_df.iloc[0]['Asset'] if not result_df.empty else None

    return result_df, str(top_asset)