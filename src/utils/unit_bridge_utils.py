import pandas as pd


def process_bridge_operations(data_dict: dict) -> pd.DataFrame | None:
    """
    process a single dictionary of bridge operations data into a structured DataFrame.
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

    # convert amts based on asset (assuming standard decimals)
    def convert_amount(row):
        amount = row['sourceAmount']
        asset = row['asset'].lower()
        decimals = {
            'btc': 8,
            'eth': 18,
            'sol': 9,
            'usdc': 6,
            'usdt': 6,
        }
        decimal_places = decimals.get(asset, 18)
        return amount / (10 ** decimal_places)

    df['amount_formatted'] = df.apply(convert_amount, axis=1)

    # filter only completed or nearly completed transactions for volume calculation
    completed_states = [
        'done', 'waitForSrcTxFinalization', 'sourceTxDiscovered']
    df_completed = df[df['state'].isin(completed_states)]

    return df_completed


def create_bridge_summary(df: pd.DataFrame):
    """
    create bridge transaction summary by asset
    """
    if df is None or df.empty:
        return None, None

    # group by asset and direction
    summary = df.groupby(['asset', 'direction']).agg({
        'amount_formatted': 'sum',
        'opCreatedAt': ['min', 'max', 'count']
    }).round(6)

    # flatten column names
    summary.columns = ['Volume', 'First_Txn', 'Last_Txn', 'Count']
    summary = summary.reset_index()

    # pivot numeric data with fill_value=0
    pivot_numeric = summary.pivot_table(
        index='asset',
        columns='direction',
        values=['Volume', 'Count'],
        fill_value=0    # TODO
    )

    # pivot datetime data with no fill_value (uses NaT by default)
    pivot_datetime = summary.pivot_table(
        index='asset',
        columns='direction',
        values=['First_Txn', 'Last_Txn']
    )

    # flatten column names for both pivot tables
    pivot_numeric.columns = [
        f'{col[0]}_{col[1]}' for col in pivot_numeric.columns]
    pivot_datetime.columns = [
        f'{col[0]}_{col[1]}' for col in pivot_datetime.columns]

    # combine the two pivot tables
    pivot_summary = pivot_numeric.join(pivot_datetime)

    result_data = []
    assets = pivot_summary.index.tolist()

    for asset in assets:
        deposit_volume = pivot_summary.get(f'Volume_Deposit', 0).get(asset, 0)
        deposit_first = pivot_summary.get(f'First_Txn_Deposit', {}).get(asset)
        deposit_last = pivot_summary.get(f'Last_Txn_Deposit', {}).get(asset)
        deposit_count = pivot_summary.get(f'Count_Deposit', 0).get(asset, 0)

        withdraw_volume = pivot_summary.get(
            f'Volume_Withdraw', 0).get(asset, 0)
        withdraw_first = pivot_summary.get(
            f'First_Txn_Withdraw', {}).get(asset)
        withdraw_last = pivot_summary.get(f'Last_Txn_Withdraw', {}).get(asset)
        withdraw_count = pivot_summary.get(f'Count_Withdraw', 0).get(asset, 0)

        # calculate overall first and last transaction
        dates = [d for d in [deposit_first, deposit_last, withdraw_first, withdraw_last] if pd.notna(d)]
        overall_first = min(dates) if dates else pd.NaT
        overall_last = max(dates) if dates else pd.NaT

        total_volume = deposit_volume + withdraw_volume
        total_count = deposit_count + withdraw_count

        result_data.append({
            'Asset': asset.upper(),
            'Deposit Volume': deposit_volume,
            'Withdraw Volume': withdraw_volume,
            'Total Volume': total_volume,
            'Total Transactions': int(total_count),
            'First Transaction': overall_first,
            'Last Transaction': overall_last
        })

    result_df = pd.DataFrame(result_data).sort_values(
        'Total Volume', ascending=False)

    # find top bridged asset
    top_asset = result_df.iloc[0]['Asset'] if not result_df.empty else None

    return result_df, top_asset
