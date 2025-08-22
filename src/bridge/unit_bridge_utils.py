from logging import Logger
import numpy as np
import pandas as pd


def process_bridge_operations(
    data_dict: dict,
    unit_token_mappings: dict[str, tuple[str, int]],
    cumulative_trade_data: pd.DataFrame,
    logger: Logger,
) -> pd.DataFrame | None:
    """
    process a single dictionary of bridge operations data into a structured df
    """
    if not data_dict or 'operations' not in data_dict or not data_dict['operations']:
        return None

    operations = data_dict['operations']
    df = pd.DataFrame(operations)

    # parse dates
    df['opCreatedAt'] = pd.to_datetime(df['opCreatedAt'])
    df['broadcastAt'] = pd.to_datetime(df['broadcastAt'], errors='coerce')

    # convert amounts to float from wei
    df['sourceAmount'] = pd.to_numeric(df['sourceAmount'], errors='coerce')
    df['destinationFeeAmount'] = pd.to_numeric(
        df['destinationFeeAmount'], errors='coerce')

    # determine txn direction (deposit vs withdraw)
    df['direction'] = df['destinationChain'].apply(
        lambda x: 'Deposit' if x == 'hyperliquid' else 'Withdraw'
    )

    amt_cols = ['amount_formatted', 'amount_usd']

    # convert amts based on asset (assuming standard decimals)
    def convert_amount(
        row,
        unit_token_mappings: dict[str, tuple[str, int]],
        cumulative_trade_data: pd.DataFrame,
        unique_tokens: list[str]
    ):
        amount = row['sourceAmount']
        asset = row['asset']

        # prices keys are UBTC, UETH etc.
        # convert to token name
        found_key = ""
        for key in unique_tokens:
            if key.lower().endswith(asset) \
                    or (key == 'UFART' and asset == 'fartcoin'):    # TODO FART must be fartcoin?
                found_key = key
                break
        if found_key == "":
            logger.warning(
                f'no matching key found for bridge asset {asset} among available unit tokens ({", ".join(unique_tokens)})')
            return pd.Series([np.nan, np.nan], index=amt_cols)

        # convert string to BOD timestamp then get closing prices of that day
        # target date from the row (treat the row timestamp as UTC)
        target_date = pd.to_datetime(row['opCreatedAt'], utc=True).date()

        # filter by token and matching calendar date
        mask = (
            (cumulative_trade_data['token_name'] == found_key) &
            (cumulative_trade_data['start_date'].dt.date == target_date)
        )
        found_row = cumulative_trade_data.loc[mask]

        if found_row.empty:
            logger.warning(
                f'target date {target_date} of {row['opCreatedAt']} not found in price list, ignoring')
            return pd.Series([np.nan, np.nan], index=amt_cols)

        # assume only have 1 day since candlestick data is fetched daily
        price = float(found_row['close_price'].iloc[0])

        decimal_places = 18
        found_decimals = False
        for asset_name, decimals in unit_token_mappings.values():
            if asset_name.lower().endswith(asset):
                decimal_places = decimals
                found_decimals = True
                break
        if not found_decimals:
            logger.warning(
                f"error: decimal places not found for {asset}: setting to default {decimal_places}")
            return pd.Series([np.nan, np.nan], index=amt_cols)

        amount_formatted = amount / (10 ** decimal_places)
        amount_usd = amount_formatted * price

        return pd.Series([amount_formatted, amount_usd], index=amt_cols)

    unique_tokens = cumulative_trade_data['token_name'].unique().tolist()
    df[amt_cols] = df.apply(lambda row: convert_amount(
        row, unit_token_mappings, cumulative_trade_data, unique_tokens), axis=1)

    # filter only completed or nearly completed transactions for volume calculation
    completed_states = [
        'done', 'waitForSrcTxFinalization', 'sourceTxDiscovered']
    df_completed = df[df['state'].isin(completed_states)]

    # filter out cols where price is null / not found
    df_completed = df_completed.dropna(subset=['amount_usd'])

    return df_completed


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
        fill_value=0,    # TODO
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
